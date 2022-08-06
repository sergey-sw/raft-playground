package xyz.skywind.raft.rpc

import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Term

data class LeaderHeartbeat(val term: Term, val leader: NodeID)