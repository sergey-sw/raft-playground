package xyz.skywind.raft.msg

import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Term

class NewLeaderMessage(override val term: Term, val leader: NodeID) : Message