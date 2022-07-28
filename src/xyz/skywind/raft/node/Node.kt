package xyz.skywind.raft.node

import xyz.skywind.raft.msg.VoteRequest
import xyz.skywind.raft.msg.VoteResponse
import xyz.skywind.raft.msg.NewLeaderMessage

interface Node {

    val nodeID: NodeID

    fun start()

    fun handle(msg: VoteRequest)

    fun handle(msg: VoteResponse)

    fun handle(msg: NewLeaderMessage)
}