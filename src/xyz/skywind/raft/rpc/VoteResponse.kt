package xyz.skywind.raft.rpc

import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.State
import xyz.skywind.raft.node.Term

data class VoteResponse(val requestTerm: Term, val granted: Boolean, val voter: NodeID, val voterTerm: Term) {

    fun voteDenied(): Boolean {
        return !granted
    }
}