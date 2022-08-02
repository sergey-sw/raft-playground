package xyz.skywind.raft.cluster

import xyz.skywind.raft.msg.*
import xyz.skywind.raft.node.Node
import xyz.skywind.raft.node.NodeID
import xyz.skywind.tools.Delay
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Level
import java.util.logging.Logger

class Network {

    companion object {
        const val DEFAULT_NETWORK_DELAY_MILLIS = 5
        const val MESSAGE_LOSS_PROBABILITY = 0.03
        const val MESSAGE_DUPLICATE_PROBABILITY = 0.03
    }

    private val nodes: MutableList<Node> = ArrayList()

    // number represents network ID
    // if two nodes have same mask value, they are connected
    private val masks: MutableMap<NodeID, Int> = HashMap()

    private var networkDelayMillis: AtomicInteger = AtomicInteger(5)

    private val logger = Logger.getLogger("network")

    private val random = Random()

    @Synchronized
    fun connect(node: Node) {
        for (n in nodes)
            if (node.nodeID == n.nodeID)
                throw IllegalArgumentException("Network already contains node $node")

        nodes.add(node)
        masks[node.nodeID] = 0
    }

    @Synchronized
    fun broadcast(from: NodeID, message: Message) {
        for (node in nodes) {
            if (node.nodeID != from) { // don't broadcast to itself
                if (connected(from, node.nodeID)) { // check nodes are connected
                    handle(from, node, message)
                }
            }
        }
    }

    @Synchronized
    fun send(from: NodeID, to: NodeID, msg: Message) {
        if (connected(from, to)) {
            for (node in nodes) {
                if (to == node.nodeID) {
                    handle(from, node, msg)
                }
            }
        }
    }

    @Synchronized
    fun randomPartition() {
        val rnd = Random()

        val numOfPartitions = rnd.nextInt(2, nodes.size + 1)

        for (nodeID in masks.keys) {
            masks[nodeID] = rnd.nextInt(numOfPartitions) // assign node to random partition
        }

        logger.log(Level.WARNING, ">> Network partition happened: ${prettifyPartitions()} <<")
    }

    @Synchronized
    fun connectAll() {
        for (nodeID in masks.keys) {
            masks[nodeID] = 0
        }

        logger.log(Level.WARNING, ">> Network partition resolved <<")
    }

    private fun prettifyPartitions(): ArrayList<List<NodeID>> {
        val partition2node = nodes.groupBy { n -> masks[n.nodeID] }

        val groups = ArrayList<List<NodeID>>()
        for (group in partition2node.values) {
            groups.add(group.map { node -> node.nodeID })
        }
        return groups
    }

    private fun connected(node1: NodeID, node2: NodeID): Boolean {
        return masks[node1] == masks[node2]
    }

    private fun handle(from: NodeID, node: Node, message: Message) {
        CompletableFuture.runAsync {
            Thread.sleep(Delay.upTo(networkDelayMillis.get()).toLong())

            if (random.nextDouble() < MESSAGE_LOSS_PROBABILITY) {
                logger.warning("Message $message from $from to ${node.nodeID} is lost")
                return@runAsync
            }

            handleMessage(node, message)

            if (random.nextDouble() < MESSAGE_DUPLICATE_PROBABILITY) {
                logger.warning("Message $message from $from to ${node.nodeID} is duplicated")
                handleMessage(node, message)
            }
        }
    }

    private fun handleMessage(node: Node, message: Message) {
        when (message) {
            is VoteRequest -> node.handle(message)
            is VoteResponse -> node.handle(message)
            is NewLeaderMessage -> node.handle(message)
            is LeaderHeartbeat -> node.handle(message)
            is HeartbeatResponse -> node.handle(message)

            else -> throw UnsupportedOperationException(message.javaClass.simpleName)
        }
    }
}