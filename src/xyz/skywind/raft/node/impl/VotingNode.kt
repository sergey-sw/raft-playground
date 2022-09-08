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
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock

open class VotingNode(
    final override val nodeID: NodeID,
    protected val config: Config,
    protected val network: Network
) : Node {

    protected val logging = LifecycleLogging(nodeID)

    protected var state = States.initialState()

    protected val data = Data(nodeID)

    protected val timerTask = TimerTask(
        { state }, config, logging,
        { maybeUpgradeFromFollowerToCandidate() },
        { stepDownFromCandidateToFollower() },
        { sendHeartbeat() }
    )

    protected val stateLock = ReentrantLock()
    protected val appendEntriesResponseCondition: Condition = stateLock.newCondition()

    override fun start() {
        logging.nodeStarted()
        timerTask.start()
    }

    override fun process(req: VoteRequest): VoteResponse {
        stateLock.lock()
        try {
            if (state.term > req.candidateTerm || !matchesCandidateLog(req)) {
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
        } finally {
            stateLock.unlock()
        }
    }

    private fun processVoteResponse(response: VoteResponse) {
        stateLock.lock()
        try {
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
                    state = States.addFollower(state, data.getLastEntry(), response.voter)
                    logging.addFollowerToLeader(state, response)
                }

                Role.CANDIDATE -> {
                    logging.candidateAcceptsVoteResponse(state, response)

                    state = States.addFollower(state, data.getLastEntry(), response.voter)
                    if (config.isQuorum(state.followers.size)) {
                        state = States.candidateBecomesLeader(state, data.getLastEntry(), response)
                        logging.leaderAfterAcceptedVote(state)
                        sendHeartbeat()
                    } else {
                        logging.candidateAfterAcceptedVote(state)
                    }
                }
            }
        } finally {
            stateLock.unlock()
        }
    }

    override fun process(req: AppendEntries): HeartbeatResponse {
        stateLock.lock()
        try {
            if (state.term > req.term || !matchesLeaderLog(req)) {
                logging.onStrangeHeartbeat(state, req)
                return HeartbeatResponse(ok = false, follower = nodeID, followerTerm = state.term)
            }

            if (state.term == req.term && state.leaderInfo?.leader == req.leader) {
                state = States.updateLeaderHeartbeatTime(state)
            } else {
                state = States.fromAnyRoleToFollower(state, req)
                logging.acceptedLeadership(req)
            }

            return HeartbeatResponse(
                ok = true,
                follower = nodeID,
                followerTerm = state.term,
                followerLastEntryIdx = handleEntries(req)
            )
        } finally {
            stateLock.unlock()
        }
    }

    protected fun processHeartbeatResponse(response: HeartbeatResponse) {
        stateLock.lock()
        try {
            if (state.role != Role.LEADER) return

            if (state.term == response.followerTerm) {
                state = States.updateFollower(response.ok, state, response.followerLastEntryIdx, response.follower)
            } else if (state.term < response.followerTerm) {
                logging.onFailedHeartbeat(state, response)
                state = States.stepDownToFollower(state, response)
                timerTask.resetElectionTimeout()
            }

            appendEntriesResponseCondition.signal()
        } finally {
            stateLock.unlock()
        }
    }

    private fun stepDownToFollowerBecauseOfHigherTerm(voteResponse: VoteResponse) {
        state = States.stepDownToFollowerBecauseOfHigherTerm(state, voteResponse.voterTerm)
        logging.stepDownToFollower(state, voteResponse)
        timerTask.resetElectionTimeout()
    }

    private fun maybeUpgradeFromFollowerToCandidate() {
        stateLock.lock()
        try {
            if (state.needSelfPromotion(config)) {
                state = States.becomeCandidate(state, nodeID) // if there's no leader yet, let's promote ourselves
                network.broadcast(
                    from = nodeID,
                    request = VoteRequest(state.term, nodeID, data.getLastEntry()),
                    callback = { processVoteResponse(it) }
                )
                logging.promotedToCandidate(state)
            }
        } finally {
            stateLock.unlock()
        }
    }

    private fun stepDownFromCandidateToFollower() {
        stateLock.lock()
        try {
            if (state.role == Role.CANDIDATE) {
                state = States.stepDownToFollowerOnElectionTimeout(state)
                logging.stepDownFromCandidateToFollower(state)
            }
        } finally {
            stateLock.unlock()
        }
    }


    private fun sendHeartbeat() {
        stateLock.lock()
        try {
            if (state.role == Role.LEADER) {
                broadcastHeartbeat()
                logging.onHeartbeatBroadcast(state)
            }
        } finally {
            stateLock.unlock()
        }
    }

    // ========== extension points for DataNode ============= /

    protected open fun broadcastHeartbeat() {
        network.broadcast(
            from = nodeID,
            requestBuilder = { AppendEntries(state, data.getLastEntry(), listOf()) },
            callback = { processHeartbeatResponse(it) }
        )
    }

    protected open fun handleEntries(request: AppendEntries): Int {
        return 0
    }

    protected open fun matchesLeaderLog(request: AppendEntries): Boolean {
        return true
    }

    protected open fun matchesCandidateLog(request: VoteRequest): Boolean {
        return true
    }
}