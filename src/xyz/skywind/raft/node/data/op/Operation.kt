package xyz.skywind.raft.node.data.op

import xyz.skywind.raft.node.model.Term

interface Operation {

    val term: Term
}