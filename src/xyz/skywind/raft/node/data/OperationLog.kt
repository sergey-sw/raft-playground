package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.Term
import xyz.skywind.raft.node.data.op.Operation

class OperationLog {

    private data class Entry(val op: Operation, val term: Term)

    private val commitIdx = -1

    private val log = ArrayList<Entry>()

    fun append(op: Operation, term: Term) {
        log.add(Entry(op, term))
    }
}