package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.data.op.Operation
import xyz.skywind.raft.node.data.op.RemoveValueOperation
import xyz.skywind.raft.node.data.op.SetValueOperation
import xyz.skywind.raft.node.model.State
import xyz.skywind.raft.rpc.AppendEntries
import java.lang.Integer.min

class Data(nodeID: NodeID) {

    private val log: OpLog = OperationLog(nodeID)

    private val kv = HashMap<String, String>()

    fun getByKey(key: String): String? {
        return kv[key]
    }

    fun getAll(): Map<String, String> {
        return HashMap(kv)
    }

    fun applyOperationsSince(commitIdx: Int) {
        log.getOperationsBetween(commitIdx, log.size()).forEach { applyOperation(it) }
    }

    private fun applyOperation(op: Operation) {
        when (op) {
            is RemoveValueOperation -> applyRemove(op)
            is SetValueOperation -> applySet(op)
        }
    }

    fun append(op: Operation) { // can be used on leader
        log.append(getLastEntry(), listOf(op))
    }

    fun append(leaderLastEntry: LogEntryInfo, operations: List<Operation>) {
        log.append(leaderLastEntry, operations)
    }

    fun getOpsFrom(fromIndex: Int): List<Operation> {
        return log.getOperationsBetween(fromIndex, log.size())
    }

    fun getLastEntry(): LogEntryInfo {
        return log.getLastEntry()
    }

    fun getEntryAt(index: Int): LogEntryInfo {
        return log.getEntryAt(index)
    }

    fun containsEntry(prevLogEntryInfo: LogEntryInfo): Boolean {
        return log.contains(prevLogEntryInfo)
    }

    fun isNotAheadOfEntry(theirPrevLogEntry: LogEntryInfo): Boolean {
        val ourLastEntry = log.getLastEntry()

        if (ourLastEntry.term != theirPrevLogEntry.term) {
            return theirPrevLogEntry.term > ourLastEntry.term
        }

        return ourLastEntry.index <= theirPrevLogEntry.index
    }

    fun maybeApplyEntries(req: AppendEntries, followerState: State): Int {
        val leaderCommitIdx = req.commitIndex
        val followerCommitIdx = followerState.commitIdx

        if (leaderCommitIdx > followerCommitIdx) {
            val newFollowerCommitIdx = min(leaderCommitIdx, log.getLastEntry().index)
            val operations = log.getOperationsBetween(followerCommitIdx + 1, newFollowerCommitIdx + 1)
            operations.forEach { applyOperation(it) }
            return operations.size
        } else {
            return 0
        }
    }

    fun dumpData(): String {
        return kv.toString()
    }

    fun dumpLog(): String {
        return log.toDetailedString()
    }

    private fun applySet(op: SetValueOperation) {
        kv[op.key] = op.value
    }

    private fun applyRemove(op: RemoveValueOperation) {
        kv.remove(op.key)
    }
}