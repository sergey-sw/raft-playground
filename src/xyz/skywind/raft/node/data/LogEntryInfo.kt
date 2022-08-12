package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.Term

data class LogEntryInfo(private val index: Int, private val term: Term) {
    companion object {
        val FIRST = LogEntryInfo(-1, Term(0))
    }
}