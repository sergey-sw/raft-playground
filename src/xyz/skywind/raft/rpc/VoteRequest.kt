package xyz.skywind.raft.rpc

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.Term

data class VoteRequest(val candidateTerm: Term, val candidate: NodeID)