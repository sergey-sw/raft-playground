package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.model.Term

data class LogEntryInfo(val index: Int, val term: Term) {
    companion object {
        val FIRST = LogEntryInfo(-1, Term(0))
    }

    override fun toString(): String {
        return "LogEntryInfo(index=$index, term=$term)"
    }
}