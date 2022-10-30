package xyz.skywind.raft

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.Term
import xyz.skywind.raft.node.data.LogEntryInfo
import xyz.skywind.raft.node.data.OperationLog
import xyz.skywind.raft.node.data.InMemoryOperationLog
import xyz.skywind.raft.node.data.op.SetValueOperation

object OperationLogTest {

    @JvmStatic
    fun main(args: Array<String>) {
        testAppendEmptyLog()
        testAppendNonEmptyLog()
        testAppendTailConflict()
        testAppendHeadConflict()
        testEmptyOpListIfFollowerMatchesLeader()

        println("Tests passed")
    }

    private fun testAppendEmptyLog() {
        val log = InMemoryOperationLog(NodeID("test"))

        val op = SetValueOperation(Term(1), "key", "value")

        log.append(log.getLastEntry(), listOf(op))

        check(log.size() == 1 + (OperationLog.START_IDX + 1))
    }

    private fun testAppendNonEmptyLog() {
        val log = InMemoryOperationLog(NodeID("test"))

        log.append(log.getLastEntry(), listOf(getOp(1), getOp(2), getOp(3)))

        log.append(log.getLastEntry(), listOf(getOp(4), getOp(5), getOp(6)))

        check(log.size() == 6 + (OperationLog.START_IDX + 1))
    }

    private fun testAppendTailConflict() {
        val log = InMemoryOperationLog(NodeID("test"))

        log.append(log.getLastEntry(), listOf(getOp(1), getOp(2), getOp(3)))

        val prevEntry = LogEntryInfo(index = 2, term = Term(1))

        log.append(prevEntry, listOf(getOp(30), getOp(40)))

        check(log.size() == 4 + (OperationLog.START_IDX + 1))
        check((log.getOperationAt(0 + (OperationLog.START_IDX + 1)) as SetValueOperation).key == "key1")
        check((log.getOperationAt(1 + (OperationLog.START_IDX + 1)) as SetValueOperation).key == "key2")
        check((log.getOperationAt(2 + (OperationLog.START_IDX + 1)) as SetValueOperation).key == "key30")
        check((log.getOperationAt(3 + (OperationLog.START_IDX + 1)) as SetValueOperation).key == "key40")
    }

    private fun testAppendHeadConflict() {
        val log = InMemoryOperationLog(NodeID("test"))

        log.append(log.getLastEntry(), listOf(getOp(1), getOp(2), getOp(3)))

        // add to head again
        log.append(log.getEntryAt(OperationLog.START_IDX), listOf(getOp(4), getOp(5)))

        check(log.size() == 2 + (OperationLog.START_IDX + 1))
        check((log.getOperationAt(0 + (OperationLog.START_IDX + 1)) as SetValueOperation).key == "key4")
        check((log.getOperationAt(1 + (OperationLog.START_IDX + 1)) as SetValueOperation).key == "key5")
    }

    private fun testEmptyOpListIfFollowerMatchesLeader() {
        val log = InMemoryOperationLog(NodeID("test"))

        log.append(log.getLastEntry(), listOf(getOp(1), getOp(2)))

        val ops = log.getOperationsBetween(2, 2)
        check(ops.isEmpty())
    }

    private fun getOp(id: Int): SetValueOperation {
        return SetValueOperation(Term(1), "key$id", "value$id")
    }
}