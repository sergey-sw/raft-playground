package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.cluster.Network
import xyz.skywind.raft.msg.NewLeaderMessage
import xyz.skywind.raft.msg.VoteRequest
import xyz.skywind.raft.msg.VoteResponse
import xyz.skywind.raft.node.RaftAssertions.verifyRequestHasHigherTerm
import xyz.skywind.raft.node.log.LifecycleLogging
import xyz.skywind.raft.node.scheduler.Scheduler
import xyz.skywind.tools.Delay
import java.util.concurrent.ScheduledFuture

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

    @Volatile
    private var selfPromotionFuture: ScheduledFuture<*>? = null

    private val periodicalTasks = PeriodicalTasks(this, config, logging)

    @Synchronized
    override fun start() {
        logging.nodeStarted()
        periodicalTasks.start()
        tryPromoteMeAsLeaderLater()
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

                    logging.stepDownToFollower(state, msg)
                    state = States.stepDownToFollower(msg)
                    network.send(msg.candidate, VoteResponse(nodeID, msg.candidate, msg.term))
                    tryPromoteMeAsLeaderLater()
                }

                Role.FOLLOWER -> {
                    state = States.voteFor(state, msg.term, msg.candidate)
                    network.send(msg.candidate, VoteResponse(nodeID, msg.candidate, msg.term))
                    logging.voted(msg)

                    // reset the election timeout, if we voted for someone in this round
                    selfPromotionFuture?.cancel(true)
                    periodicalTasks.restart()
                    tryPromoteMeAsLeaderLater()
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

    internal fun onInternalHeartbeat() {
        when (state.role) {
            Role.FOLLOWER -> {

            }

            Role.CANDIDATE -> {

            }
        }
    }

    private fun tryPromoteMeAsLeaderLater() {
        val electionTimeout = Delay.between(config.electionTimeoutMinMs, config.electionTimeoutMaxMs)
        logging.awaitingSelfPromotion(electionTimeout)

        selfPromotionFuture = scheduler.runLater(electionTimeout) {
            maybePromoteMeAsLeader()
        }
    }

    internal fun maybePromoteMeAsLeader() {
        if (state.leader == null) {
            // if there's no leader yet, let's promote ourselves
            state = States.becomeCandidate(state, nodeID)
            network.broadcast(nodeID, VoteRequest(state.term, nodeID))
            logging.promotedToCandidate(state)

            // schedule a task to fail-over if we do not receive enough responses
            scheduler.runLater(config.electionTimeoutMaxMs) {
                if (state.role == Role.CANDIDATE) {
                    // election failed, we are still just a candidate. rollback to follower state
                    state = States.fromCandidateToFollower(state)

                    tryPromoteMeAsLeaderLater()
                }
            }
        }
    }
}