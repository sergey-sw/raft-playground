package xyz.skywind.raft.cluster

import xyz.skywind.raft.rpc.*
import xyz.skywind.raft.node.Node
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.tools.Delay
import xyz.skywind.tools.Logging
import xyz.skywind.tools.Time
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import java.util.function.Function
import java.util.logging.Level

class Network(private val networkConfig: NetworkConfig) {

    private val nodes: MutableList<Node> = ArrayList()

    // Number represents a bucket. If two nodes sit in the same bucket, they are connected.
    @Volatile
    private var masks: MutableMap<NodeID, Int> = HashMap()

    private val logger = Logging.getLogger("network")

    private val random = Random()

    fun start() {
        if (networkConfig.partitionsEnabled) {
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

    fun broadcast(
        from: NodeID,
        requestBuilder: Function<NodeID, AppendEntries>,
        callback: Consumer<AppendEntriesResponse>
    ):
            List<CompletableFuture<AppendEntriesResponse?>> {

        val futures = ArrayList<CompletableFuture<AppendEntriesResponse?>>()
        for (node in nodes) {
            if (node.nodeID != from) { // don't broadcast to itself
                if (connected(from, node.nodeID)) { // check nodes are connected
                    futures += sendLeaderHeartbeat(from, node, requestBuilder.apply(node.nodeID), callback)
                }
            }
        }
        return futures
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
                Thread.sleep(Time.millis(Delay.between(5_000, 8_000)))
                randomPartition()
                Thread.sleep(Time.millis(Delay.between(100, 10_000)))
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
            if (random.nextDouble() < networkConfig.messageLossProbability) {
                logger.log(Level.WARNING, "Request $request from $from to ${node.nodeID} is lost")
                return@runAsync
            }

            execute(node, request, callback)

            if (random.nextDouble() < networkConfig.messageDuplicationProbability) {
                logger.log(Level.WARNING, "Request $request from $from to ${node.nodeID} is duplicated")
                execute(node, request, callback)
            }
        }.logErrorsTo(logger)
    }

    private fun sendLeaderHeartbeat(
        from: NodeID,
        node: Node,
        request: AppendEntries,
        callback: Consumer<AppendEntriesResponse>
    ): CompletableFuture<AppendEntriesResponse?> {
        return CompletableFuture.supplyAsync {
            if (random.nextDouble() < networkConfig.messageLossProbability) {
                logger.log(Level.WARNING, "Request $request from $from to ${node.nodeID} is lost")
                return@supplyAsync null
            }

            val response = execute(node, request, callback)

            if (random.nextDouble() < networkConfig.messageDuplicationProbability) {
                logger.log(Level.WARNING, "Request $request from $from to ${node.nodeID} is duplicated")
                execute(node, request, callback)
            }

            return@supplyAsync response
        }.logErrorsTo(logger)
    }

    private fun execute(
        node: Node,
        request: AppendEntries,
        callback: Consumer<AppendEntriesResponse>
    ): AppendEntriesResponse {
        Thread.sleep(Time.millis(Delay.upTo(networkConfig.messageDeliveryDelayMillis)))
        val response = node.process(request)
        Thread.sleep(Time.millis(Delay.upTo(networkConfig.messageDeliveryDelayMillis)))
        callback.accept(response)

        return response
    }

    private fun execute(node: Node, request: VoteRequest, callback: Consumer<VoteResponse>) {
        Thread.sleep(Time.millis(Delay.upTo(networkConfig.messageDeliveryDelayMillis)))
        val response = node.process(request)
        Thread.sleep(Time.millis(Delay.upTo(networkConfig.messageDeliveryDelayMillis)))
        callback.accept(response)
    }
}

private fun <T> CompletableFuture<T>.logErrorsTo(logger: Logging.MyLogger): CompletableFuture<T> {
    return exceptionally {
        logger.log(Level.SEVERE, it.stackTraceToString())
        null
    }
}