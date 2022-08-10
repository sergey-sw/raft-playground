package xyz.skywind.raft.node.data.op

import xyz.skywind.raft.node.Term

class SetValueOperation(override val term: Term, val key: String, val value: ByteArray): Operation