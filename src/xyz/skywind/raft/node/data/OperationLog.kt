package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.data.op.Operation

class OperationLog {

    private val log = ArrayList<Operation>()

    fun append(op: Operation): LogEntryInfo {
        val prevLogEntryInfo = getLastEntry()

        log.add(op)

        return prevLogEntryInfo
    }

    fun getLastEntry(): LogEntryInfo {
        return if (log.isEmpty())
            LogEntryInfo.FIRST
        else
            LogEntryInfo(log.size, log.last().term)
    }
}