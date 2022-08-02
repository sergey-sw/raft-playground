package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.cluster.Network
import xyz.skywind.raft.msg.*
import xyz.skywind.raft.utils.RaftAssertions.verifyRequestHasHigherTerm
import xyz.skywind.raft.node.log.LifecycleLogging
import xyz.skywind.raft.node.scheduler.PromotionTask
import xyz.skywind.raft.node.scheduler.Scheduler
import xyz.skywind.raft.utils.States

class NodeImpl(override val nodeID: NodeID, private val config: Config, private val network: Network) : Node {

    private val logging = LifecycleLogging(nodeID)

    private val scheduler = Scheduler()

    private var state = States.initialState()

    private val promotionTask = PromotionTask(
            { state.role }, config, logging, scheduler,
            { maybeUpgradeFromFollowerToCandidate() },
            { stepDownFromCandidateToFollower() },
            { sendHeartbeat() }
    )

    override fun start() {
        logging.nodeStarted()
        promotionTask.start()
    }

    override fun handle(msg: NewLeaderMessage) {
        scheduler.runNow {
            if (state.canAcceptTerm(msg.term)) {
                acceptLeadership(msg)
            } else {
                logging.ignoredLeadershipRequest(state, msg)
            }
        }
    }

    override fun handle(msg: LeaderHeartbeat) {
        scheduler.runNow {
            if (state.term == msg.term && state.leader == msg.leader) {
                state = States.updateLeaderHeartbeat(state)
                network.send(nodeID, msg.leader, HeartbeatResponse(msg.term, nodeID))
            } else if (state.canAcceptTerm(msg.term)) {
                acceptLeadership(msg)
                network.send(nodeID, msg.leader, HeartbeatResponse(msg.term, nodeID))
            } else {
                logging.onStrangeHeartbeat(state, msg)
            }
        }
    }

    override fun handle(msg: HeartbeatResponse) {
        scheduler.runNow {
            if (state.term == msg.term && state.role == Role.LEADER) {
                state = States.updateFollowerHeartbeat(state, msg.follower)
            } else {
                logging.onStrangeHeartbeatResponse(state, msg)
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

            network.send(from = nodeID, to = msg.candidate, msg = VoteResponse(nodeID, msg.candidate, msg.term))
            logging.voted(msg)
            promotionTask.resetElectionTimeout()
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
                Role.FOLLOWER -> logging.receivedVoteResponseInFollowerState(state, msg)

                Role.LEADER -> {
                    state = States.addFollower(state, msg.follower)
                    logging.addFollowerToLeader(state, msg)
                }

                Role.CANDIDATE -> {
                    logging.candidateAcceptsVoteResponse(state, msg)

                    state = States.addFollower(state, msg.follower)
                    if (config.isQuorum(state.followerHeartbeats.size)) {
                        state = States.candidateBecomesLeader(state, msg)
                        network.broadcast(nodeID, NewLeaderMessage(state.term, nodeID))
                    }
                    logging.afterAcceptedVote(state)
                }
            }
        }
    }

    private fun acceptLeadership(msg: MessageFromLeader) {
        val votedForThisLeader = (state.vote == msg.leader)
        state = States.fromAnyRoleToFollower(msg)
        if (!votedForThisLeader) {
            network.send(from = nodeID, to = msg.leader, msg = VoteResponse(nodeID, msg.leader, msg.term))
        }
        logging.acceptedLeadership(msg)
    }

    private fun maybeUpgradeFromFollowerToCandidate() { // should be called only from PromotionTask
        if (state.needSelfPromotion(config)) {
            // if there's no leader yet, let's promote ourselves
            state = States.becomeCandidate(state, nodeID)
            network.broadcast(nodeID, VoteRequest(state.term, nodeID))
            logging.promotedToCandidate(state)
        }
    }

    private fun stepDownFromCandidateToFollower() { // should be called only from PromotionTask
        state = States.stepDownToFollower(state)
        logging.stepDownFromCandidateToFollower(state)
    }

    private fun sendHeartbeat() { // should be called only from PromotionTask
        network.broadcast(nodeID, LeaderHeartbeat(state.term, nodeID))
        logging.onHeartbeatBroadcast(state)
    }
}