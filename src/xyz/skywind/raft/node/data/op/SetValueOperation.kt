package xyz.skywind.raft.node.data.op

import xyz.skywind.raft.node.model.Term

class SetValueOperation(override val term: Term, val key: String, val value: String) : Operation {
    override fun toString(): String {
        return "Set(term=$term, key='$key', value='${value}')"
    }
}