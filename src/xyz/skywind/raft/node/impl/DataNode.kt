package xyz.skywind.raft.node.impl

import xyz.skywind.raft.cluster.ClusterConfig
import xyz.skywind.raft.cluster.Network
import xyz.skywind.raft.node.data.ClientAPI
import xyz.skywind.raft.node.data.ClientAPI.*
import xyz.skywind.raft.node.data.op.Operation
import xyz.skywind.raft.node.data.op.RemoveValueOperation
import xyz.skywind.raft.node.data.op.SetValueOperation
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.Role
import xyz.skywind.raft.rpc.AppendEntries
import xyz.skywind.raft.rpc.AppendEntriesResponse
import xyz.skywind.raft.rpc.RpcUtils
import xyz.skywind.raft.rpc.VoteRequest
import xyz.skywind.raft.utils.States
import xyz.skywind.tools.Time
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class DataNode(nodeID: NodeID, clusterConfig: ClusterConfig, network: Network) : VotingNode(nodeID, clusterConfig, network), ClientAPI {

    private val mutationLock = ReentrantLock() // blocks clients from running concurrent mutations

    override fun get(key: String): GetOperationResponse {
        stateLock.lock()
        try {
            return GetOperationResponse(
                success = state.isActiveLeader(clusterConfig),
                data = data.getByKey(key),
                leaderInfo = state.leaderInfo
            )
        } finally {
            stateLock.unlock()
        }
    }

    override fun getAll(): GetAllOperationResponse {
        stateLock.lock()
        try {
            return GetAllOperationResponse(
                success = state.isActiveLeader(clusterConfig),
                data = data.getAll(),
                leaderInfo = state.leaderInfo
            )
        } finally {
            stateLock.unlock()
        }
    }

    override fun set(key: String, value: String): SetOperationResponse {
        mutationLock.lock()
        stateLock.lock()
        try {
            if (state.role != Role.LEADER) {
                return SetOperationResponse(success = false, leaderInfo = state.leaderInfo)
            }

            val success = execute(SetValueOperation(state.term, key, value))

            return SetOperationResponse(success, state.leaderInfo)
        } finally {
            stateLock.unlock()
            mutationLock.unlock()
        }
    }

    override fun remove(key: String): RemoveOperationResponse {
        mutationLock.lock()
        stateLock.lock()
        try {
            if (state.role != Role.LEADER) {
                return RemoveOperationResponse(success = false, leaderInfo = state.leaderInfo)
            }

            val success = execute(RemoveValueOperation(state.term, key))

            return RemoveOperationResponse(success, leaderInfo = state.leaderInfo)
        } finally {
            stateLock.unlock()
            mutationLock.unlock()
        }
    }

    private fun execute(operation: Operation): Boolean {
        canHeartbeat.set(false)
        logging.onBeforeAppendEntriesBroadcast(operation, data)

        val futures = network.broadcast(
            from = nodeID,
            requestBuilder = { buildAppendEntriesRequest(it, listOf(operation)) },
            callback = { processResponse(it) }
        )

        val isSuccess = clusterConfig.isQuorum(1 + getOkResponseCount(futures))
        if (isSuccess) {
            data.append(operation)
            data.applyOperationsSince(state.commitIdx)

            state = States.updateIndices(state, getLastEntryIndex())

            logging.onSuccessOperation(state, operation, data)
            timerTask.resetHeartbeatTimeout()
        } else {
            state = States.rollbackFollowerIndices(state, getLastEntryIndex())
            logging.onFailedOperation(state, operation, data)
        }

        canHeartbeat.set(true)
        return isSuccess
    }

    override fun broadcastHeartbeat() {
        network.broadcast(
            from = nodeID,
            requestBuilder = { buildAppendEntriesRequest(it, listOf()) },
            callback = { processResponse(it) }
        )
    }

    override fun handleEntries(request: AppendEntries): LastEntryIndex {
        data.append(request.lastLogEntryInfo, request.entries)

        val appliedOperationCount = data.maybeApplyEntries(request, state)

        state = States.incCommitAndAppliedIndices(state, appliedOperationCount)

        logging.onAfterAppendEntries(state, request, appliedOperationCount, data)

        return getLastEntryIndex()
    }

    override fun getLastEntryIndex(): Int {
        return data.getLastEntry().index
    }

    // Node can accept new entries from leader only if node contains leader's prev log entry
    override fun matchesLeaderLog(request: AppendEntries): Boolean {
        return data.containsEntry(request.lastLogEntryInfo).also {
            if (!it) {
                logging.onLogMismatch(request, data.dumpLog())
            }
        }
    }

    // Node can accept candidate vote request only if it's log is not ahead of candidate's log
    override fun matchesCandidateLog(request: VoteRequest): Boolean {
        return data.isNotAheadOfEntry(request.prevLogEntryInfo)
    }

    private fun buildAppendEntriesRequest(follower: NodeID, operations: List<Operation>): AppendEntries {
        val followerInfo = state.followers[follower]
            ?: return AppendEntries(state, data.getLastEntry(), operations)

        val lastLogEntryInfo = data.getEntryAt(followerInfo.nextIdx - 1)
        val newOperations = data.getOpsFrom(followerInfo.nextIdx) + operations

        return AppendEntries(state, lastLogEntryInfo, newOperations).also {
            if (it.entries.size > operations.size) {
                logging.onFollowerCatchingUp(follower, it.entries, followerInfo.nextIdx)
            }
        }
    }

    private fun getOkResponseCount(futures: List<CompletableFuture<AppendEntriesResponse?>>): Int {
        var okResponseCount = -1
        while (okResponseCount == -1) {
            appendEntriesResponseCondition.await(Time.millis(20), TimeUnit.MILLISECONDS)
            okResponseCount = RpcUtils.countSuccess(futures)
        }
        return okResponseCount
    }
}