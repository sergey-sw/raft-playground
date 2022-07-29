package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.cluster.Network
import xyz.skywind.raft.msg.NewLeaderMessage
import xyz.skywind.raft.msg.VoteRequest
import xyz.skywind.raft.msg.VoteResponse
import xyz.skywind.raft.node.scheduler.Scheduler
import xyz.skywind.tools.Delay
import java.util.concurrent.ScheduledFuture
import java.util.logging.Level
import java.util.logging.Logger

class NodeImpl(override val nodeID: NodeID, private val config: Config, private val network: Network) : Node {

    private val logger = Logger.getLogger("raft-node-$nodeID")

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

    @Synchronized
    override fun start() {
        log(Level.INFO, "Node $nodeID started")
        tryPromoteMeAsLeaderLater()
    }

    override fun handle(msg: NewLeaderMessage) {
        scheduler.runNow {
            if (state.canAcceptTerm(msg.term)) {
                state = States.fromAnyRoleToFollower(msg)
                log(Level.INFO, "Node $nodeID accepted leadership of node ${msg.leader} in term ${msg.term}")
            } else {
                log(Level.WARNING, "Node $nodeID refused leadership from ${msg.leader}. " +
                        "Current term ${state.term}, leader term: ${msg.term}")
            }
        }
    }

    override fun handle(msg: VoteRequest) {
        scheduler.runNow {
            if (state.role != Role.FOLLOWER) {
                if (msg.term > state.term) {
                    log(Level.INFO, "Stepping down from ${state.role} role in term ${state.term}: " +
                            "received vote request for term ${msg.term} from ${msg.candidate}")
                    state = States.stepDownToFollower(msg)
                    network.send(msg.candidate, VoteResponse(nodeID, msg.candidate, msg.term))
                    tryPromoteMeAsLeaderLater()
                } else {
                    log(Level.INFO, "Refused vote request from ${msg.candidate} in term ${msg.term}, " +
                            "because node is ${state.role} in term ${state.term}")
                }
                return@runNow
            } else if (state.canAcceptTerm(msg.term)) {
                if (state.votedInThisTerm(msg.term)) {
                    log(Level.INFO, "Refused vote request from ${msg.candidate} in term ${msg.term}. " +
                            "Already voted for ${state.vote}")
                    return@runNow
                }

                state = States.voteFor(state, msg.term, msg.candidate)
                network.send(msg.candidate, VoteResponse(nodeID, msg.candidate, msg.term))
                log(Level.INFO, "Voted for ${msg.candidate} in term ${msg.term}")

                // reset the election timeout, if we voted for someone in this round
                selfPromotionFuture?.cancel(true)
                tryPromoteMeAsLeaderLater()
            } else {
                log(Level.INFO, "Refused vote request from ${msg.candidate}. " +
                        "Current term ${state.term} > candidate term: ${msg.term}")
            }
        }
    }

    override fun handle(msg: VoteResponse) {
        scheduler.runNow {
            if (msg.candidate != nodeID) {
                log(Level.WARNING, "Received vote response for ${msg.candidate}. Ignoring")
                return@runNow
            } else if (state.term != msg.term) {
                log(Level.INFO, "Received vote response for term ${msg.term}, current term is ${state.term}. Ignoring")
                return@runNow
            }

            return@runNow when (state.role) {
                Role.FOLLOWER -> {
                    log(Level.INFO, "Ignoring vote response for term ${msg.term}, because current role is: ${state.role}")
                }

                Role.LEADER -> {
                    log(Level.INFO, "Received vote response from ${msg.follower}, add to followers: ${state.followers}")

                    state = State(state, followers = state.followers + msg.follower)
                }

                Role.CANDIDATE -> {
                    log(Level.INFO, "Accepting vote response in term ${state.term} from follower ${msg.follower}")

                    if (config.isQuorum(state.followers.size + 1)) {
                        state = States.candidateBecomesLeader(state, msg)
                        network.broadcast(nodeID, NewLeaderMessage(state.term, nodeID))
                        log(Level.INFO, "Node $nodeID became leader in term ${state.term} with followers: ${state.followers}")
                    } else {
                        state = State(state, followers = state.followers + msg.follower)
                        log(Level.INFO, "Node is still candidate in term ${state.term}, followers: ${state.followers}")
                    }
                }
            }
        }
    }

    private fun tryPromoteMeAsLeaderLater() {
        val electionTimeout = Delay.between(config.electionTimeoutMinMs, config.electionTimeoutMaxMs)
        log(Level.INFO, "Will wait $electionTimeout ms before trying to promote self to leader")

        selfPromotionFuture = scheduler.runLater(electionTimeout) {
            maybePromoteMeAsLeader()
        }
    }

    private fun maybePromoteMeAsLeader() {
        if (state.leader == null) {
            // if there's no leader yet, let's promote ourselves
            state = States.becomeCandidate(state, nodeID)
            network.broadcast(nodeID, VoteRequest(state.term, nodeID))
            log(Level.INFO, "Became a candidate in term ${state.term} and requested votes from others")

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

    private fun log(level: Level, msg: String) {
        logger.log(level, msg)
    }
}