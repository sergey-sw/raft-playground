package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.cluster.Network
import xyz.skywind.raft.rpc.*
import xyz.skywind.raft.utils.RaftAssertions.verifyRequestHasHigherTerm
import xyz.skywind.raft.node.log.LifecycleLogging
import xyz.skywind.raft.node.scheduler.PromotionTask
import xyz.skywind.raft.node.scheduler.Scheduler
import xyz.skywind.raft.utils.States

class NodeImpl(override val nodeID: NodeID, private val config: Config, private val network: Network) : Node {

    private val logging = LifecycleLogging(nodeID)

    // TODO how to synchronize now?
    private val scheduler = Scheduler()

    private var state = States.initialState()

    // TODO how to synchronize now?
    private val promotionTask = PromotionTask(
            { state.role }, config, logging, scheduler,
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
            logging.rejectVoteRequestBecauseOfSmallTerm(state, req)
            return VoteResponse(granted = false, requestTerm = req.candidateTerm, voter = nodeID, voterTerm = state.term)
        } else if (state.votedInThisTerm(req.candidateTerm)) {
            logging.rejectVoteRequestBecauseAlreadyVoted(state, req)
            return VoteResponse(granted = false, requestTerm = req.candidateTerm, voter = nodeID, voterTerm = state.term)
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

    private fun processVoteResponse(response: VoteResponse) {
        if (response.voteDenied()) {
            logging.onDeniedVoteResponse(state, response)
            if (response.voterTerm > state.term) {
                maybeStepDownToFollower(response)
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
                    network.broadcast(nodeID, LeaderHeartbeat(state.term, nodeID), heartbeatCallback)
                }

                logging.afterAcceptedVote(state)
            }
        }
    }

    @Synchronized
    override fun process(req: LeaderHeartbeat): HeartbeatResponse {
        if (state.term == req.term && state.leader == req.leader) {
            state = States.updateLeaderHeartbeat(state)
            return HeartbeatResponse(ok = true, follower = nodeID, followerTerm = state.term)
        } else if (state.canAcceptTerm(req.term)) {
            state = States.fromAnyRoleToFollower(req)
            logging.acceptedLeadership(req)
            promotionTask.resetElectionTimeout()
            return HeartbeatResponse(ok = true, follower = nodeID, followerTerm = state.term)
        } else {
            logging.onStrangeHeartbeat(state, req)
            return HeartbeatResponse(ok = false, follower = nodeID, followerTerm = state.term)
        }
    }

    // TODO handle response.notOK
    private fun processHeartbeatResponse(response: HeartbeatResponse) {
        if (response.ok && state.term == response.followerTerm && state.role == Role.LEADER) {
            state = States.updateFollowerHeartbeat(state, response.follower)
        } else {
            logging.onStrangeHeartbeatResponse(state, response)
        }
    }

    private fun maybeUpgradeFromFollowerToCandidate() { // should be called only from PromotionTask
        if (state.needSelfPromotion(config)) {
            state = States.becomeCandidate(state, nodeID) // if there's no leader yet, let's promote ourselves
            network.broadcast(nodeID, VoteRequest(state.term, nodeID), voteCallback)
            logging.promotedToCandidate(state)
        }
    }

    private fun maybeStepDownToFollower(voteResponse: VoteResponse) {
        if (state.role != Role.FOLLOWER) {
            state = States.stepDownToFollower(state)
            logging.stepDownToFollower(state, voteResponse)
            promotionTask.resetElectionTimeout()
        }
    }

    private fun stepDownFromCandidateToFollower() { // should be called only from PromotionTask
        state = States.stepDownToFollower(state)
        logging.stepDownFromCandidateToFollower(state)
    }

    private fun sendHeartbeat() { // should be called only from PromotionTask
        network.broadcast(nodeID, LeaderHeartbeat(state.term, nodeID), heartbeatCallback)
        logging.onHeartbeatBroadcast(state)
    }
}