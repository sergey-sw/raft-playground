package xyz.skywind.raft.msg

import xyz.skywind.raft.node.NodeID

interface MessageFromLeader: Message {

    val leader: NodeID
}