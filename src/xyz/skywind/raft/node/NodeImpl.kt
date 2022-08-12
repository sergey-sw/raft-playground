package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.cluster.Network
import xyz.skywind.raft.node.data.ClientAPI
import xyz.skywind.raft.node.data.ClientAPI.*
import xyz.skywind.raft.node.data.Data
import xyz.skywind.raft.node.data.LogEntryInfo
import xyz.skywind.raft.node.data.op.RemoveValueOperation
import xyz.skywind.raft.node.data.op.SetValueOperation
import xyz.skywind.raft.rpc.*
import xyz.skywind.raft.utils.RaftAssertions.verifyRequestHasHigherTerm
import xyz.skywind.raft.utils.States

class NodeImpl(override val nodeID: NodeID, private val config: Config, private val network: Network) : Node,
    ClientAPI {

    private val logging = LifecycleLogging(nodeID)

    private var state = States.initialState()

    private val data = Data()

    private val promotionTask = PromotionTask(
        { state }, config, logging,
        { maybeUpgradeFromFollowerToCandidate() },
        { stepDownFromCandidateToFollower() },
        { sendHeartbeat() }
    )

    override fun start() {
        logging.nodeStarted()
        promotionTask.start()
    }

    @Synchronized
    override fun get(key: String): GetOperationResponse {
        return GetOperationResponse(success = (state.role == Role.LEADER), data.getByKey(key), state.leaderInfo)
    }

    @Synchronized
    override fun set(key: String, value: ByteArray): SetOperationResponse {
        if (state.role != Role.LEADER)
            return SetOperationResponse(success = false, leaderInfo = state.leaderInfo)

        val operation = SetValueOperation(state.term, key, value)
        val prevLogEntry = data.append(operation)

        val request = AppendEntries(state, prevLogEntry, listOf(operation))
        val futures = network.broadcast(from = nodeID, request) { processHeartbeatResponse(it) }
        val success = config.isQuorum(RpcUtils.countSuccess(futures))
        // TODO ok, what now? seems like we can commit and notify followers

        return SetOperationResponse(success, state.leaderInfo)
    }

    @Synchronized
    override fun remove(key: String): RemoveOperationResponse {
        if (state.role != Role.LEADER)
            return RemoveOperationResponse(success = false, leaderInfo = state.leaderInfo)

        data.append(RemoveValueOperation(state.term, key))
        // await synchronization
        return RemoveOperationResponse(success = true, leaderInfo = state.leaderInfo)
    }

    @Synchronized
    override fun process(req: VoteRequest): VoteResponse {
        if (state.term > req.candidateTerm) {
            logging.rejectVoteRequest(state, req)
            return VoteResponse(granted = false, requestTerm = req.candidateTerm, voter = nodeID, voterTerm = state.term)
        } else if (state.votedInThisTerm(req.candidateTerm)) {
            return VoteResponse(granted = state.votedFor(req.candidate), requestTerm = state.term, voter = nodeID, voterTerm = state.term)
        }

        if (state.role == Role.FOLLOWER) {
            state = States.voteFor(state, req.candidateTerm, req.candidate)
        } else {
            verifyRequestHasHigherTerm(state, req)
            logging.steppingDownToFollower(state, req)
            state = States.stepDownToFollower(state, req)
        }
        logging.voted(req)
        promotionTask.resetElectionTimeout()

        return VoteResponse(granted = true, requestTerm = req.candidateTerm, voter = nodeID, voterTerm = state.term)
    }

    @Synchronized
    private fun processVoteResponse(response: VoteResponse) {
        if (response.voteDenied()) {
            logging.onDeniedVoteResponse(state, response)
            if (response.voterTerm > state.term && state.role != Role.FOLLOWER) {
                stepDownToFollowerBecauseOfHigherTerm(response)
            }
            return
        }

        when (state.role) {
            Role.FOLLOWER -> logging.receivedVoteResponseInFollowerState(state, response)

            Role.LEADER -> {
                state = States.addFollower(state, response.voter)
                logging.addFollowerToLeader(state, response)
            }

            Role.CANDIDATE -> {
                logging.candidateAcceptsVoteResponse(state, response)

                state = States.addFollower(state, response.voter)
                if (config.isQuorum(state.followers.size)) {
                    state = States.candidateBecomesLeader(state, response)
                    logging.leaderAfterAcceptedVote(state)
                    sendHeartbeat()
                } else {
                    logging.candidateAfterAcceptedVote(state)
                }
            }
        }
    }

    @Synchronized
    override fun process(req: AppendEntries): HeartbeatResponse {
        if (state.term == req.term && state.leaderInfo?.leader == req.leader) {
            state = States.updateLeaderHeartbeat(state)
            return HeartbeatResponse(ok = true, follower = nodeID, followerTerm = state.term)
        } else if (state.canAcceptTerm(req.term)) {
            state = States.fromAnyRoleToFollower(state, req)
            logging.acceptedLeadership(req)
            return HeartbeatResponse(ok = true, follower = nodeID, followerTerm = state.term)
        } else {
            logging.onStrangeHeartbeat(state, req)
            return HeartbeatResponse(ok = false, follower = nodeID, followerTerm = state.term)
        }
    }

    @Synchronized
    private fun processHeartbeatResponse(response: HeartbeatResponse) {
        if (response.ok && state.term == response.followerTerm && state.role == Role.LEADER) {
            state = States.addFollower(state, response.follower)
        } else if (state.term < response.followerTerm) {
            logging.onFailedHeartbeat(state, response)
            state = States.stepDownToFollower(state, response)
            promotionTask.resetElectionTimeout()
        }
    }

    private fun stepDownToFollowerBecauseOfHigherTerm(voteResponse: VoteResponse) {
        state = States.stepDownToFollowerBecauseOfHigherTerm(state, voteResponse.voterTerm)
        logging.stepDownToFollower(state, voteResponse)
        promotionTask.resetElectionTimeout()
    }

    @Synchronized
    private fun maybeUpgradeFromFollowerToCandidate() {
        if (state.needSelfPromotion(config)) {
            state = States.becomeCandidate(state, nodeID) // if there's no leader yet, let's promote ourselves
            network.broadcast(nodeID, VoteRequest(state.term, nodeID)) { processVoteResponse(it) }
            logging.promotedToCandidate(state)
        }
    }

    @Synchronized
    private fun stepDownFromCandidateToFollower() {
        if (state.role == Role.CANDIDATE) {
            state = States.stepDownToFollowerOnElectionTimeout(state)
            logging.stepDownFromCandidateToFollower(state)
        }
    }

    @Synchronized
    private fun sendHeartbeat() {
        if (state.role == Role.LEADER) {
            val msg = AppendEntries(state, data.getLastEntry(), listOf())
            network.broadcast(nodeID, msg) { processHeartbeatResponse(it) }
            logging.onHeartbeatBroadcast(state)
        }
    }
}