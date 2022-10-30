package xyz.skywind.raft.node

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.rpc.*

interface Node {

    val nodeID: NodeID

    fun start()

    fun process(req: VoteRequest): VoteResponse

    fun process(req: AppendEntries): AppendEntriesResponse
}