package xyz.skywind.raft.node.impl

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.cluster.Network
import xyz.skywind.raft.node.data.ClientAPI
import xyz.skywind.raft.node.data.ClientAPI.*
import xyz.skywind.raft.node.data.op.Operation
import xyz.skywind.raft.node.data.op.RemoveValueOperation
import xyz.skywind.raft.node.data.op.SetValueOperation
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.Role
import xyz.skywind.raft.rpc.AppendEntries
import xyz.skywind.raft.rpc.RpcUtils
import xyz.skywind.raft.rpc.VoteRequest
import xyz.skywind.raft.utils.States

class DataNode(nodeID: NodeID, config: Config, network: Network) : VotingNode(nodeID, config, network), ClientAPI {

    override fun handleEntries(req: AppendEntries) {
        data.appendOnFollower(req.prevLogEntryInfo, req.entries)

        val appliedOperationCount = data.maybeApplyEntries(req, state)

        state = States.incCommitAndAppliedIndices(state, appliedOperationCount)

        logging.onAfterAppendEntries(state, req, appliedOperationCount)
    }

    // Node can accept new entries from leader only if node contains leader's prev log entry
    override fun matchesLeaderLog(req: AppendEntries): Boolean {
        return data.containsEntry(req.prevLogEntryInfo)
    }

    // Node can accept candidate vote request only if it's log is not ahead of candidate's log
    override fun matchesCandidateLog(req: VoteRequest): Boolean {
        return data.isNotAheadOfEntry(req.prevLogEntryInfo)
    }

    @Synchronized
    override fun get(key: String): GetOperationResponse {
        return GetOperationResponse(
            success = (state.role == Role.LEADER),
            data = data.getByKey(key),
            leaderInfo = state.leaderInfo
        )
    }

    @Synchronized
    override fun set(key: String, value: ByteArray): SetOperationResponse {
        if (state.role != Role.LEADER)
            return SetOperationResponse(success = false, leaderInfo = state.leaderInfo)

        val success = execute(SetValueOperation(state.term, key, value))

        return SetOperationResponse(success, state.leaderInfo)
    }

    @Synchronized
    override fun remove(key: String): RemoveOperationResponse {
        if (state.role != Role.LEADER)
            return RemoveOperationResponse(success = false, leaderInfo = state.leaderInfo)

        val success = execute(RemoveValueOperation(state.term, key))

        return RemoveOperationResponse(success, leaderInfo = state.leaderInfo)
    }

    private fun execute(operation: Operation): Boolean {
        val prevLogEntry = data.appendOnLeader(operation)

        val request = AppendEntries(state, prevLogEntry, listOf(operation))
        val futures = network.broadcast(from = nodeID, request) { processHeartbeatResponse(it) }

        val okResponseCount = RpcUtils.countSuccess(futures)
        val success = config.isQuorum(okResponseCount)

        if (success) {
            state = States.incCommitIndex(state)

            when (operation) {
                is RemoveValueOperation -> data.applyOperation(operation)
                is SetValueOperation -> data.applyOperation(operation)
            }
            state = States.incAppliedIndex(state)
            logging.onSuccessOperation(state, operation)
        } else {
            data.removeLastOperation()
            logging.onFailedOperation(state, operation)
        }

        return success
    }
}