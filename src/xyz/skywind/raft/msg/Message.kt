package xyz.skywind.raft.msg

import xyz.skywind.raft.node.Term

sealed interface Message {

    val term: Term
}