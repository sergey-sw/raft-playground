package xyz.skywind.raft.rpc

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.Term

data class VoteResponse(val requestTerm: Term, val granted: Boolean, val voter: NodeID, val voterTerm: Term) {

    fun voteDenied(): Boolean {
        return !granted
    }
}