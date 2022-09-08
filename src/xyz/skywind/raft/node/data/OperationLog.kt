package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.data.op.Operation
import xyz.skywind.tools.Logging

// TODO need to rework how we handle prev/last entries. Pointer ops looks messy on caller side.
class OperationLog(nodeID: NodeID) : OpLog {

    private val logger = Logging.getLogger("OperationsLog-$nodeID")

    private val operations = ArrayList<Operation>()
    init {
        operations.add(Operation.FIRST)
    }

    override fun append(leaderLastEntry: LogEntryInfo, newOperations: List<Operation>) {
        if (leaderLastEntry.index == operations.lastIndex) {
            operations.addAll(newOperations)
        } else if (leaderLastEntry.index < operations.size) {
            logger.warn("Removing last ${operations.size - leaderLastEntry.index - 1} entries from log")
            while (operations.size > leaderLastEntry.index + 1) {
                val operation = operations.removeLast()
                logger.warn("Removed $operation. Log.size=${operations.size}")
            }

            check(operations.isEmpty() || leaderLastEntry.term == operations.last().term)

            this.operations.addAll(newOperations)
        } else {
            throw IllegalStateException("Can't find prev entry $leaderLastEntry in log{size=${operations.size}}")
        }
    }

    override fun getEntryAt(index: Int): LogEntryInfo {
        return LogEntryInfo(index, operations[index].term)
    }

    override fun contains(logEntry: LogEntryInfo): Boolean {
        val (index, term) = logEntry

        if (index >= operations.size) {
            return false
        }

        return operations[index].term == term
    }

    override fun getOperationsBetween(from: Int, to: Int): List<Operation> {
        return if (from == operations.size)
            emptyList()
        else
            operations.subList(from, to)
    }

    override fun getOperationAt(index: Int): Operation {
        return operations[index]
    }

    override fun size(): Int {
        return operations.size
    }

    override fun toDetailedString(): String {
        return operations.toString()
    }

    override fun toString(): String {
        return "OperationLog(log.size=${operations.size}, last=${getLastEntry()})"
    }
}