package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.Term

data class LogEntryInfo(private val index: Int, private val term: Term)