package xyz.skywind.raft.rpc

import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Term

data class VoteRequest(val candidateTerm: Term, val candidate: NodeID)