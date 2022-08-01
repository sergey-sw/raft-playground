package xyz.skywind.raft.msg

import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Term

/**
 * This message is sent from a 'follower' node that voted for the leadership of a 'candidate' node
 */
data class VoteResponse(
        val follower: NodeID,
        val candidate: NodeID,
        override val term: Term) : Message {

    init {
        if (follower == candidate)
            throw IllegalArgumentException("Should not create a vote for itself")
    }
}
