package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.data.op.Operation

interface OperationLog {

    companion object {
        const val START_IDX = 0
    }

    fun append(leaderLastEntry: LogEntryInfo, newOperations: List<Operation>)

    fun contains(logEntry: LogEntryInfo): Boolean
    fun getEntryAt(index: Int): LogEntryInfo
    fun getLastEntry(): LogEntryInfo {
        return getEntryAt(size() - 1)
    }

    fun getOperationAt(index: Int): Operation
    fun getOperationsBetween(from: Int, to: Int): List<Operation>

    fun size(): Int

    fun toDetailedString(): String
}