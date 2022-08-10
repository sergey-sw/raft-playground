package xyz.skywind.raft.rpc

import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Term
import xyz.skywind.raft.node.data.op.Operation

data class AppendEntries(val term: Term, val leader: NodeID, val entries: List<Operation> = listOf())