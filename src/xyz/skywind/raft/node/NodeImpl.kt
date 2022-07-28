package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.cluster.Network
import xyz.skywind.raft.msg.NewLeaderMessage
import xyz.skywind.raft.msg.VoteRequest
import xyz.skywind.raft.msg.VoteResponse
import xyz.skywind.raft.node.scheduler.Scheduler
import xyz.skywind.tools.Delay
import xyz.skywind.tools.Time
import java.util.concurrent.ScheduledFuture
import java.util.logging.Level
import java.util.logging.Logger

class NodeImpl(override val nodeID: NodeID, config: Config, network: Network) : Node {

    private val logger: Logger

    private val config: Config
    private val network: Network
    private var state: State = State(Term(0), "null", Role.FOLLOWER, null, 0, setOf())
    private val scheduler: Scheduler

    @Volatile
    private var selfPromotionFuture: ScheduledFuture<*>? = null

    init {
        this.config = config
        this.network = network
        this.scheduler = Scheduler()

        this.logger = Logger.getLogger("raft-node-$nodeID")
    }

    @Synchronized
    override fun start() {
        log(Level.INFO, "Node $nodeID started")
        tryPromoteMeAsLeaderLater()
    }

    override fun handle(msg: NewLeaderMessage) {
        scheduler.runNow {
            if (msg.term >= state.term) {
                acceptLeadershipOf(msg.leader, msg.term)
            } else {
                log(Level.WARNING, "Node $nodeID refused leadership from ${msg.leader}. " +
                        "Current term ${state.term}, leader term: ${msg.term}")
            }
        }
    }

    override fun handle(msg: VoteRequest) {
        scheduler.runNow {
            if (state.role == Role.FOLLOWER && state.term <= msg.term) {
                network.send(msg.candidate, VoteResponse(nodeID, msg.candidate, msg.term))
                log(Level.INFO, "Voted for ${msg.candidate}, term: ${msg.term}")

                // reset the election timeout, if we voted for someone in this round
                selfPromotionFuture?.cancel(true)
                tryPromoteMeAsLeaderLater()
            } else {
                log(Level.INFO, "Refused vote request from ${msg.candidate}. " +
                        "Current term ${state.term}, candidate term: ${msg.term}")
            }
        }
    }

    override fun handle(msg: VoteResponse) {
        scheduler.runNow {
            if (msg.candidate != nodeID) {
                log(Level.WARNING, "Received vote response for ${msg.candidate}. Ignoring")
                return@runNow
            }

            if (state.role != Role.CANDIDATE) {
                if (state.role == Role.FOLLOWER) {
                    log(Level.INFO, "Received vote response, but current role is: ${state.role}. Ignoring")
                } else if (state.role == Role.LEADER) {
                    state = State(state.term, state.data, Role.LEADER, state.leader, state.lastLeaderHeartbeatTs, state.followers + msg.follower)
                    log(Level.INFO, "Received vote response from ${msg.follower}, add to followers: ${state.followers}")
                }
                return@runNow
            }

            if (state.term != msg.term) {
                log(Level.INFO, "Received vote response for term $msg.term, current term is $state.term. Ignoring")
                return@runNow
            }

            log(Level.INFO, "Accepting vote response from follower ${msg.follower}")

            if (config.isQuorum(state.followers.size + 1)) {
                state = State(state.term.incr(), state.data, Role.LEADER, nodeID, Time.now(), state.followers + msg.follower)
                network.broadcast(nodeID, NewLeaderMessage(state.term, nodeID))
                log(Level.INFO, "Node $nodeID became leader with followers: ${state.followers}")
            } else {
                state = State(state.term, state.data, Role.CANDIDATE, state.leader, state.lastLeaderHeartbeatTs, state.followers + msg.follower)
                log(Level.INFO, "Node is still candidate, followers: ${state.followers}")
            }
        }
    }

    private fun tryPromoteMeAsLeaderLater() {
        val electionTimeout = Delay.between(config.electionTimeoutMinMs, config.electionTimeoutMaxMs)

        selfPromotionFuture = scheduler.runLater(electionTimeout) {
            maybePromoteMeAsLeader()
        }
    }

    private fun maybePromoteMeAsLeader() {
        if (state.leader == null) {
            // if there's no leader yet, let's promote ourselves
            state = State(state.term.incr(), state.data, Role.CANDIDATE, null, state.lastLeaderHeartbeatTs, setOf(nodeID))
            network.broadcast(nodeID, VoteRequest(state.term, nodeID))
            log(Level.INFO, "Became a candidate and requested votes from others")

            // schedule a task to fail-over if we do not receive enough responses
            scheduler.runLater(config.electionTimeoutMaxMs) {
                if (state.role == Role.CANDIDATE) {
                    // election failed, we are still just a candidate. rollback to follower state
                    state = State(state.term.decr(), state.data, Role.FOLLOWER, state.leader, state.lastLeaderHeartbeatTs, setOf())

                    tryPromoteMeAsLeaderLater()
                }
            }
        }
    }

    private fun acceptLeadershipOf(leader: NodeID, term: Term) {
        state = State(term, state.data, Role.FOLLOWER, leader, Time.now(), setOf())
        log(Level.INFO, "Node $nodeID accepted leadership of node $leader")
    }

    private fun log(level: Level, msg: String) {
        logger.log(level, msg)
    }
}