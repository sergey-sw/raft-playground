package xyz.skywind.raft.node.log

import xyz.skywind.raft.rpc.*
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

    fun acceptedLeadership(msg: LeaderHeartbeat) {
        log(Level.INFO, "Node $nodeID received ${msg.javaClass.simpleName} and accepted " +
                "leadership of node ${msg.leader} in term ${msg.term}")
    }

    fun steppingDownToFollower(state: State, msg: VoteRequest) {
        log(Level.INFO, "Stepping down from ${state.role} role in term ${state.term}: " +
                "received vote request for term ${msg.candidateTerm} from ${msg.candidate}")
    }

    fun rejectVoteRequestBecauseOfSmallTerm(state: State, msg: VoteRequest) {
        log(Level.INFO, "Rejecting vote request from ${msg.candidate} in term ${msg.candidateTerm}, " +
                "because node is ${state.role} in term ${state.term}")
    }

    fun rejectVoteRequestBecauseAlreadyVoted(state: State, msg: VoteRequest) {
        log(Level.INFO, "Refused vote request from ${msg.candidate} in term ${msg.candidateTerm}. " +
                "Already voted for ${state.vote}")
    }

    /*fun receivedVoteResponseForOtherNode(msg: VoteResponse) {
        log(Level.WARNING, "Received vote response for ${msg.candidate}. Ignoring")
    }*/

    /*fun receivedVoteResponseForOtherTerm(state: State, msg: VoteResponse) {
        log(Level.INFO, "Received vote response for term ${msg.term}, current term is ${state.term}. Ignoring")
    }*/

    /*fun receivedVoteResponseInFollowerState(state: State, msg: VoteResponse) {
        log(Level.INFO, "Ignoring vote response for term ${msg.term}, because node is: ${state.role} in term ${state.term}")
    }*/

    fun receivedVoteResponseInFollowerState(state: State, response: VoteResponse) {
        log(Level.INFO, "Ignoring {granted=${response.granted}} VoteResponse for term ${response.requestTerm}, " +
                "because node is: ${state.role} in term ${state.term}")
    }

    /*fun addFollowerToLeader(state: State, msg: VoteResponse) {
        log(Level.INFO, "Received vote response from ${msg.follower}, add to followers: ${state.followers()}")
    }*/

    fun addFollowerToLeader(state: State, response: VoteResponse) {
        log(Level.INFO, "Received VoteResponse from ${response.voter}, add to followers: ${state.followers()}")
    }

    fun onDeniedVoteResponse(state: State, response: VoteResponse) {
        log(Level.INFO, "Received denied VoteResponse from ${response.voter} in term ${response.voterTerm}. " +
                "Current node is ${state.role} in term ${state.term}")
    }

    /*fun leaderAcceptsVoteResponse(state: State, response: VoteResponse) {
        if (response.granted) {
            log(Level.INFO, "Received VoteResponse from ${response.voter}, add to followers: ${state.followers()}")
        } else {
            log(Level.INFO, "Received VoteResponse{granted=false} from ${response.voter}, current followers: ${state.followers()}")
        }
    }*/

    /*fun candidateAcceptsVoteResponse(state: State, msg: VoteResponse) {
        log(Level.INFO, "Accepting vote response in term ${state.term} from follower ${msg.follower}")
    }*/

    fun candidateAcceptsVoteResponse(state: State, response: VoteResponse) {
        log(Level.INFO, "Accepting VoteResponse{granted=${response.granted}} in term ${state.term} from follower ${response.voter}")
    }

    fun afterAcceptedVote(state: State) {
        check(state.role != Role.FOLLOWER) { "Expected to be ${Role.LEADER} or ${Role.FOLLOWER}" }
        if (state.role == Role.CANDIDATE)
            log(Level.INFO, "Node is still candidate in term ${state.term}, followers: ${state.followers()}")
        else if (state.role == Role.LEADER)
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

    fun onStrangeHeartbeatResponse(state: State, msg: HeartbeatResponse) {
        log(Level.INFO, "Ignoring unexpected heartbeat response $msg, being a ${state.role} in term ${state.term}")
    }
}