package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.cluster.Network
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
            { maybeDegradeFromCandidateToFollower() }
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

    override fun handle(msg: VoteRequest) {
        scheduler.runNow {
            if (state.term > msg.term) {
                logging.rejectVoteRequestBecauseOfSmallTerm(state, msg)
                return@runNow
            } else if (state.votedInThisTerm(msg.term)) {
                logging.rejectVoteRequestBecauseAlreadyVoted(state, msg)
                return@runNow
            }

            return@runNow when (state.role) {
                Role.CANDIDATE, Role.LEADER -> {
                    verifyRequestHasHigherTerm(state, msg)
                    logging.steppingDownToFollower(state, msg)
                    state = States.stepDownToFollower(msg)
                    network.send(msg.candidate, VoteResponse(nodeID, msg.candidate, msg.term))
                }

                Role.FOLLOWER -> {
                    state = States.voteFor(state, msg.term, msg.candidate)
                    network.send(msg.candidate, VoteResponse(nodeID, msg.candidate, msg.term))
                    logging.voted(msg)
                    promotionTask.restart(needFullTimeout = false) // reset the election timeout when vote for someone
                }
            }
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
        scheduler.runNow {
            check(state.role == Role.FOLLOWER) { "Expected to be a FOLLOWER, when promotion timer exceeds" }

            if (state.leader == null) {
                // if there's no leader yet, let's promote ourselves
                state = States.becomeCandidate(state, nodeID)
                network.broadcast(nodeID, VoteRequest(state.term, nodeID))
                logging.promotedToCandidate(state)
            }
        }
    }

    private fun maybeDegradeFromCandidateToFollower() {
        scheduler.runNow {
            if (state.role == Role.CANDIDATE) {
                state = States.fromCandidateToFollower(state) // didn't get enough votes, become a FOLLOWER
                logging.degradedToFollower(state)
            } else {
                logging.onFailedDegradeFromCandidateToFollower(state)
            }
        }
    }
}