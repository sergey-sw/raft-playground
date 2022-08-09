package xyz.skywind.raft.node

import xyz.skywind.raft.rpc.*

interface Node {

    val nodeID: NodeID

    fun start()

    fun process(req: VoteRequest): VoteResponse

    fun process(req: LeaderHeartbeat): HeartbeatResponse
}