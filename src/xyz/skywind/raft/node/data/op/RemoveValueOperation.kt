package xyz.skywind.raft.node.data.op

import xyz.skywind.raft.node.model.Term

class RemoveValueOperation(override val term: Term, val key: String): Operation