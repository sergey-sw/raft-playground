package xyz.skywind.raft.node.data.op

import xyz.skywind.raft.node.model.Term

sealed interface Operation {

    companion object {
        val FIRST = RemoveValueOperation(term = Term(0), key = "*")
    }

    val term: Term
}