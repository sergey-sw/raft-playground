package xyz.skywind.raft.node.data.op

import xyz.skywind.raft.node.Term

class RemoveValueOperation(override val term: Term, val key: String): Operation