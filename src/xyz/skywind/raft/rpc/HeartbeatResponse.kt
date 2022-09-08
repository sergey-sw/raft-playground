package xyz.skywind.raft.rpc

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.Term

data class HeartbeatResponse(
    val ok: Boolean, val followerTerm: Term, val follower: NodeID,
    val followerLastEntryIdx: Int = 0
)