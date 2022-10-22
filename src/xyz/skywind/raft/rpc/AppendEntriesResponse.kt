package xyz.skywind.raft.rpc

import xyz.skywind.raft.node.impl.LastEntryIndex
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.Term

data class AppendEntriesResponse(
    val ok: Boolean,
    val followerTerm: Term,
    val follower: NodeID,
    val followerLastEntryIdx: LastEntryIndex = 0
)