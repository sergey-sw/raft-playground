package xyz.skywind.raft.node.impl

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.cluster.Network
import xyz.skywind.raft.node.Node
import xyz.skywind.raft.node.data.Data
import xyz.skywind.raft.node.debug.LifecycleLogging
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.Role
import xyz.skywind.raft.rpc.AppendEntries
import xyz.skywind.raft.rpc.HeartbeatResponse
import xyz.skywind.raft.rpc.VoteRequest
import xyz.skywind.raft.rpc.VoteResponse
import xyz.skywind.raft.utils.RaftAssertions
import xyz.skywind.raft.utils.States

open class VotingNode(
    final override val nodeID: NodeID,
    protected val config: Config,
    protected val network: Network) : Node {

    protected val logging = LifecycleLogging(nodeID)

    protected var state = States.initialState()

    protected val data = Data(nodeID)

    private val timerTask = TimerTask(
        { state }, config, logging,
        { maybeUpgradeFromFollowerToCandidate() },
        { stepDownFromCandidateToFollower() },
        { sendHeartbeat() }
    )

    override fun start() {
        logging.nodeStarted()
        timerTask.start()
    }

    @Synchronized
    override fun process(req: VoteRequest): VoteResponse {
        if (state.term > req.candidateTerm) {
            logging.rejectVoteRequest(state, req)
            return VoteResponse(
                granted = false,
                requestTerm = req.candidateTerm,
                voter = nodeID,
                voterTerm = state.term
            )
        } else if (state.votedInThisTerm(req.candidateTerm)) {
            return VoteResponse(
                granted = state.votedFor(req.candidate),
                requestTerm = state.term,
                voter = nodeID,
                voterTerm = state.term
            )
        }

        state = if (state.role == Role.FOLLOWER) {
            States.voteFor(state, req.candidateTerm, req.candidate)
        } else {
            RaftAssertions.verifyRequestHasHigherTerm(state, req)
            logging.steppingDownToFollower(state, req)
            States.stepDownToFollower(state, req)
        }
        logging.voted(req)
        timerTask.resetElectionTimeout()

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
                    state = States.candidateBecomesLeader(state, data.getLastEntry(), response)
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
        if (state.term > req.term || !data.matchesLeaderLog(req)) {
            logging.onStrangeHeartbeat(state, req)
            return HeartbeatResponse(ok = false, follower = nodeID, followerTerm = state.term)
        }

        if (state.term == req.term && state.leaderInfo?.leader == req.leader) {
            state = States.updateLeaderHeartbeat(state)
        } else {
            state = States.fromAnyRoleToFollower(state, req)
            logging.acceptedLeadership(req)
        }
        data.appendOnFollower(req.prevLogEntryInfo, req.entries)

        return HeartbeatResponse(ok = true, follower = nodeID, followerTerm = state.term)
    }

    @Synchronized
    protected fun processHeartbeatResponse(response: HeartbeatResponse) {
        if (response.ok && state.term == response.followerTerm && state.role == Role.LEADER) {
            state = States.addFollower(state, response.follower)
        } else if (state.term < response.followerTerm) {
            logging.onFailedHeartbeat(state, response)
            state = States.stepDownToFollower(state, response)
            timerTask.resetElectionTimeout()
        }
    }

    private fun stepDownToFollowerBecauseOfHigherTerm(voteResponse: VoteResponse) {
        state = States.stepDownToFollowerBecauseOfHigherTerm(state, voteResponse.voterTerm)
        logging.stepDownToFollower(state, voteResponse)
        timerTask.resetElectionTimeout()
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