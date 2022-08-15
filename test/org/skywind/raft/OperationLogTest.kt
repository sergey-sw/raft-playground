package org.skywind.raft

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.Term
import xyz.skywind.raft.node.data.LogEntryInfo
import xyz.skywind.raft.node.data.OperationLog
import xyz.skywind.raft.node.data.op.SetValueOperation

object OperationLogTest {

    @JvmStatic
    fun main(args: Array<String>) {
        testAppendEmptyLog()
        testAppendNonEmptyLog()
        testAppendTailConflict()
        testAppendHeadConflict()

        println("Tests passed")
    }

    private fun testAppendEmptyLog() {
        val log = OperationLog(NodeID("test"))

        val op = SetValueOperation(Term(1), "key", "value")

        log.append(LogEntryInfo.FIRST, listOf(op))

        check(log.size() == 1)
    }

    private fun testAppendNonEmptyLog() {
        val log = OperationLog(NodeID("test"))

        log.append(LogEntryInfo.FIRST, listOf(getOp(1), getOp(2), getOp(3)))

        log.append(log.getLastEntry(), listOf(getOp(4), getOp(5), getOp(6)))

        check(log.size() == 6)
    }

    private fun testAppendTailConflict() {
        val log = OperationLog(NodeID("test"))

        log.append(LogEntryInfo.FIRST, listOf(getOp(1), getOp(2), getOp(3)))

        val prevEntry = LogEntryInfo(index = 1, term = Term(1))

        log.append(prevEntry, listOf(getOp(30), getOp(40)))

        check(log.size() == 4)
        check((log.get(0) as SetValueOperation).key == "key1")
        check((log.get(1) as SetValueOperation).key == "key2")
        check((log.get(2) as SetValueOperation).key == "key30")
        check((log.get(3) as SetValueOperation).key == "key40")
    }

    private fun testAppendHeadConflict() {
        val log = OperationLog(NodeID("test"))

        log.append(LogEntryInfo.FIRST, listOf(getOp(1), getOp(2), getOp(3)))

        // add to head again
        log.append(LogEntryInfo.FIRST, listOf(getOp(4), getOp(5)))

        check(log.size() == 2)
        check((log.get(0) as SetValueOperation).key == "key4")
        check((log.get(1) as SetValueOperation).key == "key5")
    }

    private fun getOp(id: Int): SetValueOperation {
        return SetValueOperation(Term(1), "key$id", "value$id")
    }
}