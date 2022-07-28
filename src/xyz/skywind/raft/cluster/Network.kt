package xyz.skywind.raft.cluster

import xyz.skywind.raft.msg.*
import xyz.skywind.raft.node.Node
import xyz.skywind.raft.node.NodeID
import xyz.skywind.tools.Delay
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max

class Network {

    companion object {
        const val DEFAULT_NETWORK_DELAY_MILLIS = 5
    }

    private val nodes: MutableSet<Node> = HashSet()

    private var networkDelayMillis: AtomicInteger = AtomicInteger(5)

    fun connect(node: Node) {
        for (n in nodes)
            if (node.nodeID == n.nodeID)
                throw java.lang.IllegalArgumentException("Network already contains node $node")

        nodes.add(node)
    }

    fun setNetworkDelay(delayMillis: Int) {
        networkDelayMillis.set(max(delayMillis, DEFAULT_NETWORK_DELAY_MILLIS))
    }

    fun broadcast(from: NodeID, message: Message) {
        for (node in nodes) {
            if (node.nodeID != from)
                handle(node, message)
        }
    }

    fun send(to: NodeID, message: Message) {
        for (node in nodes) {
            if (to == node.nodeID)
                handle(node, message)
        }
    }

    private fun handle(node: Node, message: Message) {
        CompletableFuture.runAsync {
            Thread.sleep(Delay.upTo(networkDelayMillis.get()).toLong())

            when (message) {
                is VoteRequest -> node.handle(message)
                is VoteResponse -> node.handle(message)
                is NewLeaderMessage -> node.handle(message)
            }
        }
    }
}