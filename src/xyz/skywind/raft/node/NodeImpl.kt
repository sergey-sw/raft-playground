package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.cluster.Network
import xyz.skywind.raft.rpc.*
import xyz.skywind.raft.utils.RaftAssertions.verifyRequestHasHigherTerm
import xyz.skywind.raft.utils.States

class NodeImpl(override val nodeID: NodeID, private val config: Config, private val network: Network) : Node {

    private val logging = LifecycleLogging(nodeID)

    @Volatile
    private var state = States.initialState()

    private val promotionTask = PromotionTask(
            { state }, config, logging,
            { maybeUpgradeFromFollowerToCandidate() },
            { stepDownFromCandidateToFollower() },
            { sendHeartbeat() }
    )

    private val voteCallback = { response: VoteResponse -> processVoteResponse(response) }
    private val heartbeatCallback = { response: HeartbeatResponse -> processHeartbeatResponse(response) }

    override fun start() {
        logging.nodeStarted()
        promotionTask.start()
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
            state = States.stepDownToFollower(req)
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
                if (config.isQuorum(state.followerHeartbeats.size)) {
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
    override fun process(req: LeaderHeartbeat): HeartbeatResponse {
        if (state.term == req.term && state.leaderInfo?.leader == req.leader) {
            state = States.updateLeaderHeartbeat(state)
            return HeartbeatResponse(ok = true, follower = nodeID, followerTerm = state.term)
        } else if (state.canAcceptTerm(req.term)) {
            state = States.fromAnyRoleToFollower(req)
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
            state = States.updateFollowerHeartbeat(state, response.follower)
        } else if (state.term < response.followerTerm) {
            logging.onFailedHeartbeat(state, response)
            state = States.stepDownToFollower(response)
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
            network.broadcast(nodeID, VoteRequest(state.term, nodeID), voteCallback)
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
            network.broadcast(nodeID, LeaderHeartbeat(state.term, nodeID), heartbeatCallback)
            logging.onHeartbeatBroadcast(state)
        }
    }
}