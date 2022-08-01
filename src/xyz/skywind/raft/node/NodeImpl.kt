package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.cluster.Network
import xyz.skywind.raft.msg.LeaderHeartbeat
import xyz.skywind.raft.msg.NewLeaderMessage
import xyz.skywind.raft.msg.VoteRequest
import xyz.skywind.raft.msg.VoteResponse
import xyz.skywind.raft.utils.RaftAssertions.verifyRequestHasHigherTerm
import xyz.skywind.raft.node.log.LifecycleLogging
import xyz.skywind.raft.node.scheduler.PromotionTask
import xyz.skywind.raft.node.scheduler.Scheduler
import xyz.skywind.raft.utils.States

class NodeImpl(override val nodeID: NodeID, private val config: Config, private val network: Network) : Node {

    private val logging = LifecycleLogging(nodeID)

    private val scheduler = Scheduler()

    private var state = State(
            term = Term(0),
            vote = null,
            role = Role.FOLLOWER,
            leader = null,
            lastLeaderHeartbeatTs = 0,
            followers = setOf()
    )

    private val promotionTask = PromotionTask(
            { state.role }, config, logging, scheduler,
            { maybeUpgradeFromFollowerToCandidate() },
            { degradeFromCandidateToFollower() },
            { sendHeartbeat() }
    )

    override fun start() {
        logging.nodeStarted()
        promotionTask.start()
    }

    override fun handle(msg: NewLeaderMessage) {
        scheduler.runNow {
            if (state.canAcceptTerm(msg.term)) {
                state = States.fromAnyRoleToFollower(msg)
                logging.acceptedLeadershipRequest(msg)
            } else {
                logging.ignoredLeadershipRequest(state, msg)
            }
        }
    }

    override fun handle(msg: LeaderHeartbeat) {
        scheduler.runNow {
            if (state.term == msg.term && state.leader == msg.leader) {
                state = States.updateHeartbeat(state)
            } else {
                logging.onStrangeHeartbeat(state, msg)
            }
        }
    }

    override fun handle(msg: VoteRequest) {
        scheduler.runNow {
            if (state.term > msg.term) {
                logging.rejectVoteRequestBecauseOfSmallTerm(state, msg)
                return@runNow
            } else if (state.votedInThisTerm(msg.term)) {
                logging.rejectVoteRequestBecauseAlreadyVoted(state, msg)
                return@runNow
            }

            if (state.role == Role.FOLLOWER) {
                state = States.voteFor(state, msg.term, msg.candidate)
            } else {
                verifyRequestHasHigherTerm(state, msg)
                logging.steppingDownToFollower(state, msg)
                state = States.stepDownToFollower(msg)
            }

            network.send(msg.candidate, VoteResponse(nodeID, msg.candidate, msg.term))
            logging.voted(msg)
            promotionTask.restart(needFullTimeout = false) // reset the election timeout when vote for someone
        }
    }

    override fun handle(msg: VoteResponse) {
        scheduler.runNow {
            if (msg.candidate != nodeID) {
                logging.receivedVoteResponseForOtherNode(msg)
                return@runNow
            } else if (state.term != msg.term) {
                logging.receivedVoteResponseForOtherTerm(state, msg)
                return@runNow
            }

            return@runNow when (state.role) {
                Role.FOLLOWER -> {
                    logging.receivedVoteResponseInFollowerState(state, msg)
                }

                Role.LEADER -> {
                    state = States.addFollower(state, msg.follower)
                    logging.addFollowerToLeader(state, msg)
                }

                Role.CANDIDATE -> {
                    logging.candidateAcceptsVoteResponse(state, msg)

                    state = States.addFollower(state, msg.follower)
                    if (config.isQuorum(state.followers.size)) {
                        state = States.candidateBecomesLeader(state, msg)
                        network.broadcast(nodeID, NewLeaderMessage(state.term, nodeID))
                    }
                    logging.afterAcceptedVote(state)
                }
            }
        }
    }

    private fun maybeUpgradeFromFollowerToCandidate() {
        check(state.role == Role.FOLLOWER) { "Expected to be a FOLLOWER, when promotion timer exceeds" }
        if (state.needSelfPromotion(config)) {
            // if there's no leader yet, let's promote ourselves
            state = States.becomeCandidate(state, nodeID)
            network.broadcast(nodeID, VoteRequest(state.term, nodeID))
            logging.promotedToCandidate(state)
        }
    }

    private fun degradeFromCandidateToFollower() {
        if (state.role == Role.CANDIDATE) {
            state = States.fromCandidateToFollower(state) // didn't get enough votes, become a FOLLOWER
            logging.degradedToFollower(state)
        } else {
            logging.onFailedDegradeFromCandidateToFollower(state)
        }
    }

    private fun sendHeartbeat() {
        check(state.role == Role.LEADER) { "Expected to be a LEADER, when sending heartbeats" }
        network.broadcast(nodeID, LeaderHeartbeat(state.term, nodeID))
        logging.onHeartbeatBroadcast(state)
    }
}