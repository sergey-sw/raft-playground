package xyz.skywind.raft.node

import xyz.skywind.raft.msg.*

interface Node {

    val nodeID: NodeID

    fun start()

    fun handle(msg: VoteRequest)

    fun handle(msg: VoteResponse)

    fun handle(msg: LeaderHeartbeat)

    fun handle(msg: HeartbeatResponse)
}