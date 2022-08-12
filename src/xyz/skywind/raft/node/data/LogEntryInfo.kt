package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.Term

data class LogEntryInfo(val index: Int, val term: Term) {
    companion object {
        val FIRST = LogEntryInfo(-1, Term(0))
    }
}