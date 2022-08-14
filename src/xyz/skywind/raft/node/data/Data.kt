package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.data.op.Operation
import xyz.skywind.raft.node.data.op.RemoveValueOperation
import xyz.skywind.raft.node.data.op.SetValueOperation
import xyz.skywind.raft.node.model.State
import xyz.skywind.raft.rpc.AppendEntries
import xyz.skywind.tools.Logging
import java.lang.Integer.min
import java.util.logging.Level

class Data(nodeID: NodeID) {

    companion object {
        val EMPTY_VALUE = ByteArray(0)
    }

    private val logger = Logging.getLogger("Data-$nodeID")

    private val log = OperationLog(nodeID)

    private val kv = HashMap<String, ByteArray>()

    fun getByKey(key: String): ByteArray {
        return kv[key] ?: EMPTY_VALUE
    }

    @Synchronized
    fun applyOperation(op: SetValueOperation) {
        applySet(op)
    }

    @Synchronized
    fun applyOperation(op: RemoveValueOperation) {
        applyRemove(op)
    }

    @Synchronized
    fun appendOnLeader(op: Operation): LogEntryInfo {
        return log.append(op)
    }

    @Synchronized
    fun removeLastOperation() {
        return log.removeLast()
    }

    @Synchronized
    fun appendOnFollower(prevLogEntryInfo: LogEntryInfo, entries: List<Operation>) {
        log.append(prevLogEntryInfo, entries)
    }

    @Synchronized
    fun getLastEntry(): LogEntryInfo {
        return log.getLastEntry()
    }

    @Synchronized
    fun containsEntry(prevLogEntryInfo: LogEntryInfo): Boolean {
        val matches = log.contains(prevLogEntryInfo)

        if (!matches) {
            logger.log(
                Level.WARNING, "Node last entry ${getLastEntry()} does not match with " +
                        "request prev entry $prevLogEntryInfo"
            )
        }

        return matches
    }

    @Synchronized
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
            for (operation in operations) {
                when (operation) {
                    is SetValueOperation -> applySet(operation)
                    is RemoveValueOperation -> applyRemove(operation)
                }
            }
            return operations.size
        } else {
            return 0
        }
    }

    private fun applySet(op: SetValueOperation) {
        kv[op.key] = op.value
    }

    private fun applyRemove(op: RemoveValueOperation) {
        kv.remove(op.key)
    }
}