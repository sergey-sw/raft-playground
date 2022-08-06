package xyz.skywind.raft.rpc

import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Term

/**
 * This message is sent from a node in CANDIDATE status to other nodes in a cluster
 */
data class VoteRequest(val candidateTerm: Term, val candidate: NodeID)