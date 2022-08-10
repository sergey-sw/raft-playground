package xyz.skywind.raft.node

import xyz.skywind.raft.rpc.*
import xyz.skywind.tools.Logging
import java.util.logging.Level

class LifecycleLogging(private val nodeID: NodeID) {

    private val logger = Logging.getLogger("raft-node-$nodeID")

    private fun log(level: Level, msg: String) {
        logger.log(level, msg)
    }

    fun nodeStarted() {
        log(Level.INFO, "Node $nodeID started")
    }

    fun acceptedLeadership(msg: LeaderHeartbeat) {
        log(Level.INFO, "Node $nodeID received ${msg.javaClass.simpleName} and accepted " +
                "leadership of node ${msg.leader} in term ${msg.term}")
    }

    fun steppingDownToFollower(state: State, msg: VoteRequest) {
        log(Level.INFO, "Stepping down from ${state.role} role in term ${state.term}: " +
                "received vote request for term ${msg.candidateTerm} from ${msg.candidate}")
    }

    fun rejectVoteRequest(state: State, msg: VoteRequest) {
        log(Level.INFO, "Rejecting VoteRequest from ${msg.candidate} in term ${msg.candidateTerm}. " +
                "Node is ${state.role} in term ${state.term}, votedFor=${state.voteInfo?.vote}")
    }

    fun receivedVoteResponseInFollowerState(state: State, response: VoteResponse) {
        log(Level.INFO, "Ignoring {granted=${response.granted}} VoteResponse for term ${response.requestTerm}, " +
                "because node is: ${state.role} in term ${state.term}")
    }

    fun addFollowerToLeader(state: State, response: VoteResponse) {
        log(Level.INFO, "Received VoteResponse for term ${response.requestTerm} from ${response.voter}, " +
                "add to followers: ${state.followers()}")
    }

    fun onDeniedVoteResponse(state: State, response: VoteResponse) {
        log(Level.INFO, "Received VoteResponse{granted=false} for term ${response.requestTerm} " +
                "from ${response.voter} in term ${response.voterTerm}. " +
                "Current role is ${state.role}")
    }

    fun candidateAcceptsVoteResponse(state: State, response: VoteResponse) {
        log(Level.INFO, "Accepting VoteResponse{granted=${response.granted}} in term ${state.term} from follower ${response.voter}")
    }

    fun candidateAfterAcceptedVote(state: State) {
        check(state.role == Role.CANDIDATE)
        log(Level.INFO, "Node is still candidate in term ${state.term}, followers: ${state.followers()}")
    }

    fun leaderAfterAcceptedVote(state: State) {
        check(state.role == Role.LEADER)
        log(Level.INFO, "Node $nodeID became leader in term ${state.term} with followers: ${state.followers()}")
    }

    fun awaitingSelfPromotion(electionTimeout: Number) {
        log(Level.INFO, "Will wait $electionTimeout ms before promoting self to candidate")
    }

    fun promotedToCandidate(state: State) {
        log(Level.INFO, "Became a candidate in term ${state.term} and requested votes from others")
    }

    fun stepDownFromCandidateToFollower(state: State) {
        log(Level.INFO, "Didn't get enough votes, step down to ${Role.FOLLOWER} at term ${state.term}")
    }

    fun stepDownToFollower(state: State, voteResponse: VoteResponse) {
        log(Level.INFO, "Step down to ${Role.FOLLOWER} at term ${state.term}, because of VoteResponse from " +
                "${voteResponse.voter} in term ${voteResponse.voterTerm}")

    }

    fun voted(msg: VoteRequest) {
        log(Level.INFO, "Voted for ${msg.candidate} in term ${msg.candidateTerm}")
    }

    fun onStrangeHeartbeat(state: State, msg: LeaderHeartbeat) {
        log(Level.WARNING, "Received strange leader heartbeat $msg. Node is ${state.role} in term ${state.term}")
    }

    fun onHeartbeatBroadcast(state: State) {
        log(Level.INFO, "Sent leader heartbeat in term ${state.term}. " +
                "Follower delays: ${state.lastResponseFromFollowers()}")
    }

    fun onFailedHeartbeat(state: State, response: HeartbeatResponse) {
        log(Level.INFO, "Received heartbeat from ${response.follower} at term ${response.followerTerm}. " +
                "Current term is ${state.term}. Stepping down to ${Role.FOLLOWER}")
    }
}