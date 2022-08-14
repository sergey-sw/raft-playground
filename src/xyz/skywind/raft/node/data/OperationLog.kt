package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.data.op.Operation
import xyz.skywind.tools.Logging
import java.util.logging.Level

class OperationLog(nodeID: NodeID) {

    private val logger = Logging.getLogger("OperationsLog-$nodeID")

    private val log = ArrayList<Operation>()

    fun removeLast() {
        log.removeLast()
    }

    fun append(op: Operation): LogEntryInfo {
        val prevLogEntryInfo = getLastEntry()

        log.add(op)

        return prevLogEntryInfo
    }

    fun append(prevLogEntryInfo: LogEntryInfo, entries: List<Operation>) {
        val index = prevLogEntryInfo.index

        check(index < log.size) { "Can't find prev entry $prevLogEntryInfo in log{size=${log.size}}" }

        if (index + 1 == log.size) {
            log.addAll(entries)
        } else {
            logger.log(Level.WARNING, "Removing last ${log.size - index - 1} entries from log")
            while (log.size > index + 1) {
                val operation = log.removeLast()
                logger.log(Level.WARNING, "Removed $operation")
            }

            check(log.isEmpty() || prevLogEntryInfo.term == log.last().term)

            log.addAll(entries)
        }
    }

    fun getLastEntry(): LogEntryInfo {
        return if (log.isEmpty())
            LogEntryInfo.FIRST
        else
            LogEntryInfo(log.lastIndex, log.last().term)
    }

    fun contains(logEntry: LogEntryInfo): Boolean {
        val (index, term) = logEntry

        if (index > log.size) {
            return false
        }

        if (logEntry == LogEntryInfo.FIRST) {
            return true
        }

        return log[index].term == term
    }

    fun getOperationsBetween(from: Int, to: Int): List<Operation> {
        return log.subList(from, to)
    }

    fun get(index: Int): Operation {
        return log[index]
    }

    fun size(): Int {
        return log.size
    }

    override fun toString(): String {
        return "OperationLog(log.size=${log.size}, last=${getLastEntry()})"
    }
}