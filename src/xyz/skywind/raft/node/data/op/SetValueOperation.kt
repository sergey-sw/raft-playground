package xyz.skywind.raft.node.data.op

import xyz.skywind.raft.node.model.Term

class SetValueOperation(override val term: Term, val key: String, val value: ByteArray): Operation {
    override fun toString(): String {
        return "SetValueOperation(term=$term, key='$key', value=[arr.length=${value.size}])"
    }
}