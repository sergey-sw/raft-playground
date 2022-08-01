package xyz.skywind.raft.node.log

import xyz.skywind.raft.msg.NewLeaderMessage
import xyz.skywind.raft.msg.VoteRequest
import xyz.skywind.raft.msg.VoteResponse
import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Role
import xyz.skywind.raft.node.State
import java.util.logging.Level
import java.util.logging.Logger

class LifecycleLogging(private val nodeID: NodeID) {

    private val logger = Logger.getLogger("raft-node-$nodeID")

    private fun log(level: Level, msg: String) {
        logger.log(level, msg)
    }

    fun nodeStarted() {
        log(Level.INFO, "Node $nodeID started")
    }

    fun acceptedLeadershipRequest(msg: NewLeaderMessage) {
        log(Level.INFO, "Node $nodeID accepted leadership of node ${msg.leader} in term ${msg.term}")
    }

    fun ignoredLeadershipRequest(state: State, msg: NewLeaderMessage) {
        log(Level.WARNING, "Node $nodeID refused leadership from ${msg.leader}. " +
                "Current term ${state.term}, leader term: ${msg.term}")
    }

    fun steppingDownToFollower(state: State, msg: VoteRequest) {
        log(Level.INFO, "Stepping down from ${state.role} role in term ${state.term}: " +
                "received vote request for term ${msg.term} from ${msg.candidate}")
    }

    fun rejectVoteRequestBecauseOfSmallTerm(state: State, msg: VoteRequest) {
        log(Level.INFO, "Rejecting vote request from ${msg.candidate} in term ${msg.term}, " +
                "because node is ${state.role} in term ${state.term}")
    }

    fun rejectVoteRequestBecauseAlreadyVoted(state: State, msg: VoteRequest) {
        log(Level.INFO, "Refused vote request from ${msg.candidate} in term ${msg.term}. " +
                "Already voted for ${state.vote}")
    }

    fun receivedVoteResponseForOtherNode(msg: VoteResponse) {
        log(Level.WARNING, "Received vote response for ${msg.candidate}. Ignoring")
    }

    fun receivedVoteResponseForOtherTerm(state: State, msg: VoteResponse) {
        log(Level.INFO, "Received vote response for term ${msg.term}, current term is ${state.term}. Ignoring")
    }

    fun receivedVoteResponseInFollowerState(state: State, msg: VoteResponse) {
        log(Level.INFO, "Ignoring vote response for term ${msg.term}, because node is: ${state.role} in term ${state.term}")
    }

    fun addFollowerToLeader(state: State, msg: VoteResponse) {
        log(Level.INFO, "Received vote response from ${msg.follower}, add to followers: ${state.followers}")
    }

    fun candidateAcceptsVoteResponse(state: State, msg: VoteResponse) {
        log(Level.INFO, "Accepting vote response in term ${state.term} from follower ${msg.follower}")
    }

    fun afterAcceptedVote(state: State) {
        check(state.role != Role.FOLLOWER) { "Expected to be ${Role.LEADER} or ${Role.FOLLOWER}" }
        if (state.role == Role.CANDIDATE)
            log(Level.INFO, "Node is still candidate in term ${state.term}, followers: ${state.followers}")
        else if (state.role == Role.LEADER)
            log(Level.INFO, "Node $nodeID became leader in term ${state.term} with followers: ${state.followers}")
    }

    fun awaitingSelfPromotion(electionTimeout: Number) {
        log(Level.INFO, "Will wait $electionTimeout ms before trying to promote self to leader")
    }

    fun promotedToCandidate(state: State) {
        log(Level.INFO, "Became a candidate in term ${state.term} and requested votes from others")
    }

    fun degradedToFollower(state: State) {
        log(Level.INFO, "Didn't get enough votes, step down to ${Role.FOLLOWER} at term ${state.term}")
    }

    fun voted(msg: VoteRequest) {
        log(Level.INFO, "Voted for ${msg.candidate} in term ${msg.term}")
    }

    fun onFailedDegradeFromCandidateToFollower(state: State) {
        logger.log(Level.INFO, "Node didn't receive enough votes and reached promotion timeout. Expected to be " +
                "${Role.CANDIDATE}, but node is ${state.role} in ${state.term} term. " +
                "Probably received NewLeaderMessage. Skipped candidate->follower degrade operation.")
    }
}