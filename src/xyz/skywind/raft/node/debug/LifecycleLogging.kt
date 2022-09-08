package xyz.skywind.raft.node.debug

import xyz.skywind.raft.node.data.Data
import xyz.skywind.raft.node.data.op.Operation
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.Role
import xyz.skywind.raft.node.model.State
import xyz.skywind.raft.rpc.*
import xyz.skywind.tools.Logging
import java.util.logging.Level

// TODO add state.role [F/C/L] to each message
class LifecycleLogging(private val nodeID: NodeID) {

    private val logger = Logging.getLogger("raft-node-$nodeID")

    private fun log(level: Level, msg: String) {
        logger.log(level, msg)
    }

    fun nodeStarted() {
        log(Level.INFO, "Node $nodeID started")
    }

    fun acceptedLeadership(msg: AppendEntries) {
        log(Level.INFO, "Node $nodeID received $msg and accepted leadership of node ${msg.leader} in term ${msg.term}")
    }

    fun steppingDownToFollower(state: State, msg: VoteRequest) {
        log(
            Level.INFO, "Stepping down from ${state.role} role in term ${state.term}: " +
                    "received vote request for term ${msg.candidateTerm} from ${msg.candidate}"
        )
    }

    fun rejectVoteRequest(state: State, msg: VoteRequest) {
        log(
            Level.INFO, "Rejecting VoteRequest from ${msg.candidate} in term ${msg.candidateTerm}. " +
                    "Node is ${state.role} in term ${state.term}, votedFor=${state.voteInfo?.votedFor}"
        )
    }

    fun receivedVoteResponseInFollowerState(state: State, response: VoteResponse) {
        log(
            Level.INFO, "Ignoring {granted=${response.granted}} VoteResponse from ${response.voter} for " +
                    "term ${response.requestTerm}, because node is: ${state.role} in term ${state.term}"
        )
    }

    fun addFollowerToLeader(state: State, response: VoteResponse) {
        log(
            Level.INFO, "Received VoteResponse for term ${response.requestTerm} from ${response.voter}, " +
                    "add to followers: ${state.followers()}"
        )
    }

    fun onDeniedVoteResponse(state: State, response: VoteResponse) {
        log(
            Level.INFO, "Received VoteResponse{granted=false} for term ${response.requestTerm} " +
                    "from ${response.voter} in term ${response.voterTerm}. " +
                    "Current role is ${state.role}"
        )
    }

    fun candidateAcceptsVoteResponse(state: State, response: VoteResponse) {
        log(
            Level.INFO,
            "Accepting VoteResponse{granted=${response.granted}} in term ${state.term} from follower ${response.voter}"
        )
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
        log(
            Level.INFO, "Step down to ${Role.FOLLOWER} at term ${state.term}, because of VoteResponse from " +
                    "${voteResponse.voter} in term ${voteResponse.voterTerm}"
        )

    }

    fun voted(msg: VoteRequest) {
        log(Level.INFO, "Voted for ${msg.candidate} in term ${msg.candidateTerm}")
    }

    fun onStrangeHeartbeat(state: State, msg: AppendEntries) {
        log(Level.WARNING, "Received strange $msg. Node is ${state.role} in term ${state.term}")
    }

    fun onHeartbeatBroadcast(state: State) {
        log(
            Level.INFO, "Sent leader heartbeat in term ${state.term}. " +
                    "Follower delays: ${state.lastResponseFromFollowers()}"
        )
    }

    fun onFailedHeartbeat(state: State, response: HeartbeatResponse) {
        log(
            Level.INFO, "Received heartbeat from ${response.follower} at term ${response.followerTerm}. " +
                    "Current term is ${state.term}. Stepping down to ${Role.FOLLOWER}"
        )
    }

    fun onSuccessOperation(state: State, operation: Operation, data: Data) {
        log(
            Level.INFO, "Successfully executed $operation. CommitIdx: ${state.commitIdx}, " +
                    "AppliedIdx: ${state.appliedIdx}, KV: ${data.dumpData()}"
        )
    }

    fun onFailedOperation(state: State, operation: Operation, data: Data) {
        log(
            Level.WARNING, "Failed to execute $operation. CommitIdx: ${state.commitIdx}, " +
                    "AppliedIdx: ${state.appliedIdx}, KV: ${data.dumpData()}"
        )
    }

    fun onBeforeAppendEntriesBroadcast(operation: Operation, data: Data) {
        log(Level.INFO, "Broadcasting $operation, prevLogEntry=${data.getLastEntry()}, op log: ${data.dumpLog()}")
    }

    fun onAfterAppendEntries(state: State, req: AppendEntries, appliedOperationCount: Int, data: Data) {
        if (req.entries.isNotEmpty()) {
            log(Level.INFO, "Added ${req.entries} to log")
            if (appliedOperationCount == 0) {
                log(
                    Level.INFO, "No ops were applied. Leader commit idx: ${req.commitIndex}, " +
                            "follower commit idx: ${state.commitIdx}, apply idx: ${state.appliedIdx}. KV: ${data.dumpData()}"
                )
            } else {
                log(
                    Level.INFO, "Applied $appliedOperationCount log entries. Leader commit idx: ${req.commitIndex}, " +
                            "follower commit idx: ${state.commitIdx}, apply idx: ${state.appliedIdx}. KV: ${data.dumpData()}"
                )
            }
        }
    }

    fun onFollowerCatchingUp(nodeID: NodeID, entries: List<Operation>) {
        log(Level.INFO, "Sending ${entries.size} ops to follower $nodeID: $entries")
    }

    fun onLogMismatch(req: AppendEntries, opLog: String) {
        log(Level.WARNING, "Node log does not match leader's prev entry ${req.lastLogEntryInfo}. Log: $opLog")
    }
}