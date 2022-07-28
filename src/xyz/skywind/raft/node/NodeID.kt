package xyz.skywind.raft.node

data class NodeID(val id: String) {

    override fun toString(): String {
        return id
    }
}
