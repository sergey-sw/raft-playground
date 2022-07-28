package xyz.skywind.raft.msg

import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Term

/**
 * This message is sent from a node in CANDIDATE status to other nodes in a cluster
 */
class VoteRequest(override val term: Term, val candidate: NodeID) : Message