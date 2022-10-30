package xyz.skywind.raft.cluster

import xyz.skywind.raft.node.Node
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.tools.Logging
import java.util.logging.Level

class Cluster<NodeType : Node>(private val clusterConfig: ClusterConfig, private val networkConfig: NetworkConfig) {

    private val nodes = HashSet<NodeType>()

    val network = Network(networkConfig)

    fun add(node: NodeType) {
        for (n in nodes) {
            check(n.nodeID != node.nodeID) { "Cluster already contains node $node" }
        }

        nodes.add(node)
        network.connect(node)
    }

    fun getAnyNode(): NodeType {
        return nodes.random()
    }

    fun getNode(nodeID: NodeID): NodeType {
        for (node in nodes)
            if (nodeID == node.nodeID)
                return node

        throw IllegalArgumentException("Can't find node $nodeID")
    }

    fun start() {
        val logger = Logging.getLogger("raft-cluster")

        logger.log(Level.INFO, "Starting raft cluster")
        logger.log(Level.INFO, "Nodes: " + nodes.map { n -> n.nodeID })
        logger.log(Level.INFO, "Network message delay millis: " + networkConfig.messageDeliveryDelayMillis)
        logger.log(Level.INFO, "Network message loss probability: " + networkConfig.messageLossProbability)
        logger.log(
            Level.INFO,
            "Network message duplication probability: " + networkConfig.messageDuplicationProbability
        )
        logger.log(
            Level.INFO,
            "Raft election delay millis: " + clusterConfig.electionTimeoutMinMs + ".." + clusterConfig.electionTimeoutMaxMs
        )
        logger.log(Level.INFO, "Raft heartbeat timeout millis: " + clusterConfig.heartbeatTimeoutMs)

        nodes.forEach { n -> n.start() }

        network.start()
    }
}