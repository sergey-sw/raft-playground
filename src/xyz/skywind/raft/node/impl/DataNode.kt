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
import xyz.skywind.raft.rpc.HeartbeatResponse
import xyz.skywind.raft.rpc.RpcUtils
import xyz.skywind.raft.rpc.VoteRequest
import xyz.skywind.raft.utils.States
import xyz.skywind.tools.Logging
import xyz.skywind.tools.Time
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.logging.Level

class DataNode(nodeID: NodeID, config: Config, network: Network) : VotingNode(nodeID, config, network), ClientAPI {

    override fun handleEntries(req: AppendEntries) {
        data.appendOnFollower(req.prevLogEntryInfo, req.entries)

        val appliedOperationCount = data.maybeApplyEntries(req, state)

        state = States.incCommitAndAppliedIndices(state, appliedOperationCount)

        logging.onAfterAppendEntries(state, req, appliedOperationCount, data)
    }

    // Node can accept new entries from leader only if node contains leader's prev log entry
    override fun matchesLeaderLog(req: AppendEntries): Boolean {
        return data.containsEntry(req.prevLogEntryInfo)
    }

    // Node can accept candidate vote request only if it's log is not ahead of candidate's log
    override fun matchesCandidateLog(req: VoteRequest): Boolean {
        return data.isNotAheadOfEntry(req.prevLogEntryInfo)
    }

    override fun get(key: String): GetOperationResponse {
        lock.lock()
        try {
            return GetOperationResponse(
                success = (state.role == Role.LEADER),
                data = data.getByKey(key),
                leaderInfo = state.leaderInfo
            )
        } finally {
            lock.unlock()
        }
    }

    override fun set(key: String, value: String): SetOperationResponse {
        lock.lock()
        try {
            if (state.role != Role.LEADER)
                return SetOperationResponse(success = false, leaderInfo = state.leaderInfo)

            val success = execute(SetValueOperation(state.term, key, value))

            return SetOperationResponse(success, state.leaderInfo)
        } finally {
            lock.unlock()
        }
    }

    override fun remove(key: String): RemoveOperationResponse {
        lock.lock()
        try {
            if (state.role != Role.LEADER)
                return RemoveOperationResponse(success = false, leaderInfo = state.leaderInfo)

            val success = execute(RemoveValueOperation(state.term, key))

            return RemoveOperationResponse(success, leaderInfo = state.leaderInfo)
        } finally {
            lock.unlock()
        }
    }

    private fun execute(operation: Operation): Boolean {
        val prevLogEntry = data.appendOnLeader(operation)

        val request = AppendEntries(state, prevLogEntry, listOf(operation))
        logging.onBeforeAppendEntries(operation)
        val futures = network.broadcast(from = nodeID, request) { processHeartbeatResponse(it) }

        val success = config.isQuorum(getOkResponseCount(futures))
        if (success) {
            state = States.incCommitIndex(state)

            when (operation) {
                is RemoveValueOperation -> data.applyOperation(operation)
                is SetValueOperation -> data.applyOperation(operation)
            }
            state = States.incAppliedIndex(state)
            logging.onSuccessOperation(state, operation, data)
        } else {
            data.removeLastOperation()
            logging.onFailedOperation(state, operation, data)
        }

        return success
    }

    // Hacky concurrency tricks, need to review later.
    //
    // Problem (deadlock):
    // Current thread (leader) holds a lock in #set/#remove method.
    // Because of that, no one can execute any other methods on leader.
    // When followers reply to AppendEntries broadcast, Network runs a callback on leader in CompletableFuture async method
    // This completable future will block on the lock and wait forever.
    //
    // Solution:
    // Leader thread awaits on the condition (sleeps) and releases the lock.
    // CompletableFuture threads now can run callbacks on leader. They signal on condition after the job is done.
    // Leader awakes and checks if all callbacks were executed (otherwise awaits on condition more)
    private fun getOkResponseCount(futures: List<CompletableFuture<HeartbeatResponse?>>): Int {
        val startTime = Time.now()

        var okResponseCount = -1
        var iteration = 0
        while (okResponseCount == -1) {
            appendEntriesResponseCondition.await(20, TimeUnit.MILLISECONDS)
            okResponseCount = RpcUtils.countSuccess(futures)
            iteration++
        }

        val spent = Time.now() - startTime
        Logging.getLogger("hack").log(Level.INFO, "Got AE response in $iteration iterations in $spent ms")

        return okResponseCount
    }
}