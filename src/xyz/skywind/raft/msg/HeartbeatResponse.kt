package xyz.skywind.raft.msg

import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Term

data class HeartbeatResponse(override val term: Term, val follower: NodeID) : Message