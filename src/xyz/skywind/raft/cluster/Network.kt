package xyz.skywind.raft.cluster

import xyz.skywind.raft.rpc.*
import xyz.skywind.raft.node.Node
import xyz.skywind.raft.node.NodeID
import xyz.skywind.tools.Delay
import xyz.skywind.tools.Logging
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import java.util.logging.Level

class Network {

    companion object {
        const val MESSAGE_DELIVERY_DELAY_MILLIS = 5
        const val MESSAGE_LOSS_PROBABILITY = 0
        const val MESSAGE_DUPLICATION_PROBABILITY = 0
        const val PARTITIONS_ENABLED = true
    }

    private val nodes: MutableList<Node> = ArrayList()

    // number represents network ID
    // if two nodes have same mask value, they are connected
    @Volatile
    private var masks: MutableMap<NodeID, Int> = HashMap()

    private val logger = Logging.getLogger("network")

    private val random = Random()

    fun start() {
        if (PARTITIONS_ENABLED) {
            startNetworkPartitioner()
        }
    }

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
                    sendLeaderHeartbeat(from, node, request, callback)
                }
            }
        }
    }

    fun broadcast(from: NodeID, request: VoteRequest, callback: Consumer<VoteResponse>) {
        for (node in nodes) {
            if (node.nodeID != from) { // don't broadcast to itself
                if (connected(from, node.nodeID)) { // check nodes are connected
                    sendVoteRequest(from, node, request, callback)
                }
            }
        }
    }

    private fun startNetworkPartitioner() {
        Thread {
            while (true) {
                Thread.sleep(Delay.between(5_000, 8_000).toLong())
                randomPartition()
                Thread.sleep(Delay.between(100, 10_000).toLong())
                connectAll()
            }
        }.start()
    }

    private fun randomPartition() {
        val rnd = Random()

        val numOfPartitions = rnd.nextInt(2, nodes.size + 1)

        val newMask = HashMap<NodeID, Int>()
        for (nodeID in masks.keys) {
            newMask[nodeID] = rnd.nextInt(numOfPartitions) // assign node to random partition
        }
        masks = newMask

        logger.log(Level.WARNING, ">> Network partition happened: ${prettifyPartitions()} <<")
    }

    private fun connectAll() {
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

    private fun sendVoteRequest(from: NodeID, node: Node, request: VoteRequest, callback: Consumer<VoteResponse>) {
        CompletableFuture.runAsync {
            if (random.nextDouble() < MESSAGE_LOSS_PROBABILITY) {
                logger.log(Level.WARNING, "Request $request from $from to ${node.nodeID} is lost")
                return@runAsync
            }

            execute(node, request, callback)

            if (random.nextDouble() < MESSAGE_DUPLICATION_PROBABILITY) {
                logger.log(Level.WARNING, "Request $request from $from to ${node.nodeID} is duplicated")
                execute(node, request, callback)
            }
        }.logErrorsTo(logger)
    }

    private fun sendLeaderHeartbeat(from: NodeID, node: Node, request: LeaderHeartbeat, callback: Consumer<HeartbeatResponse>) {
        CompletableFuture.runAsync {
            if (random.nextDouble() < MESSAGE_LOSS_PROBABILITY) {
                logger.log(Level.WARNING, "Request $request from $from to ${node.nodeID} is lost")
                return@runAsync
            }

            execute(node, request, callback)

            if (random.nextDouble() < MESSAGE_DUPLICATION_PROBABILITY) {
                logger.log(Level.WARNING, "Request $request from $from to ${node.nodeID} is duplicated")
                execute(node, request, callback)
            }
        }.logErrorsTo(logger)
    }

    private fun execute(node: Node, request: LeaderHeartbeat, callback: Consumer<HeartbeatResponse>) {
        Thread.sleep(Delay.upTo(MESSAGE_DELIVERY_DELAY_MILLIS).toLong())
        val response = node.process(request)
        Thread.sleep(Delay.upTo(MESSAGE_DELIVERY_DELAY_MILLIS).toLong())
        callback.accept(response)
    }

    private fun execute(node: Node, request: VoteRequest, callback: Consumer<VoteResponse>) {
        Thread.sleep(Delay.upTo(MESSAGE_DELIVERY_DELAY_MILLIS).toLong())
        val response = node.process(request)
        Thread.sleep(Delay.upTo(MESSAGE_DELIVERY_DELAY_MILLIS).toLong())
        callback.accept(response)
    }
}

private fun <T> CompletableFuture<T>.logErrorsTo(logger: Logging.MyLogger) {
    exceptionally {
        logger.log(Level.SEVERE, it.stackTraceToString())
        null
    }
}