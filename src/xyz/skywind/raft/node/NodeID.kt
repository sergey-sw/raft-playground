package xyz.skywind.raft.node

@JvmInline
value class NodeID(private val id: String) {

    override fun toString(): String {
        return id
    }
}
