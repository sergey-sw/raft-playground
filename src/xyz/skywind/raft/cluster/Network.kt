package xyz.skywind.raft.cluster

import xyz.skywind.raft.rpc.*
import xyz.skywind.raft.node.Node
import xyz.skywind.raft.node.NodeID
import xyz.skywind.tools.Delay
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Consumer
import java.util.logging.Level
import java.util.logging.Logger

class Network {

    companion object {
        const val MESSAGE_DELIVERY_DELAY_MILLIS = 5
        const val MESSAGE_LOSS_PROBABILITY = 0.03
        const val MESSAGE_DUPLICATION_PROBABILITY = 0.03
    }

    private val nodes: MutableList<Node> = ArrayList()

    // number represents network ID
    // if two nodes have same mask value, they are connected
    @Volatile
    private var masks: MutableMap<NodeID, Int> = HashMap()

    private var networkDelayMillis = AtomicInteger(5)

    private val logger = Logger.getLogger("network")

    private val random = Random()

    fun connect(node: Node) {
        for (n in nodes)
            if (node.nodeID == n.nodeID)
                throw IllegalArgumentException("Network already contains node $node")

        nodes.add(node)
        masks[node.nodeID] = 0
    }

    fun broadcast(from: NodeID, request: LeaderHeartbeat, callback: Consumer<HeartbeatResponse>) {
        for (node in nodes) {
            if (node.nodeID != from) { // don't broadcast to itself
                if (connected(from, node.nodeID)) { // check nodes are connected
                    processLeaderHeartbeat(from, node, request, callback)
                }
            }
        }
    }

    fun broadcast(from: NodeID, request: VoteRequest, callback: Consumer<VoteResponse>) {
        for (node in nodes) {
            if (node.nodeID != from) { // don't broadcast to itself
                if (connected(from, node.nodeID)) { // check nodes are connected
                    processVoteRequest(from, node, request, callback)
                }
            }
        }
    }

    fun randomPartition() {
        val rnd = Random()

        val numOfPartitions = rnd.nextInt(2, nodes.size + 1)

        val newMask = HashMap<NodeID, Int>()
        for (nodeID in masks.keys) {
            newMask[nodeID] = rnd.nextInt(numOfPartitions) // assign node to random partition
        }
        masks = newMask

        logger.log(Level.WARNING, ">> Network partition happened: ${prettifyPartitions()} <<")
    }

    fun connectAll() {
        val newMask = HashMap<NodeID, Int>()
        for (nodeID in masks.keys) {
            newMask[nodeID] = 0
        }
        masks = newMask

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
        val masks = this.masks

        return masks[node1] == masks[node2]
    }

    private fun processVoteRequest(from: NodeID, node: Node, request: VoteRequest, callback: Consumer<VoteResponse>) {
        CompletableFuture.runAsync {
            Thread.sleep(Delay.upTo(networkDelayMillis.get()).toLong())

            if (random.nextDouble() < MESSAGE_LOSS_PROBABILITY) {
                logger.warning("Request $request from $from to ${node.nodeID} is lost")
                return@runAsync
            }

            callback.accept(node.process(request))

            if (random.nextDouble() < MESSAGE_DUPLICATION_PROBABILITY) {
                logger.warning("Request $request from $from to ${node.nodeID} is duplicated")
                callback.accept(node.process(request))
            }
        }
    }

    private fun processLeaderHeartbeat(from: NodeID, node: Node, request: LeaderHeartbeat, callback: Consumer<HeartbeatResponse>) {
        CompletableFuture.runAsync {
            Thread.sleep(Delay.upTo(networkDelayMillis.get()).toLong())

            if (random.nextDouble() < MESSAGE_LOSS_PROBABILITY) {
                logger.warning("Request $request from $from to ${node.nodeID} is lost")
                return@runAsync
            }

            callback.accept(node.process(request))

            if (random.nextDouble() < MESSAGE_DUPLICATION_PROBABILITY) {
                logger.warning("Request $request from $from to ${node.nodeID} is duplicated")
                callback.accept(node.process(request))
            }
        }
    }
}