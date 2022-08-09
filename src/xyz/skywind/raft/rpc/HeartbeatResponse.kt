package xyz.skywind.raft.rpc

import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Term

data class HeartbeatResponse(val ok: Boolean, val followerTerm: Term, val follower: NodeID)