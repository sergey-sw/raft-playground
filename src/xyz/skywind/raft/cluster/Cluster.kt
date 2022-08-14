package xyz.skywind.raft.cluster

import xyz.skywind.raft.node.data.ClientAPI
import xyz.skywind.raft.node.impl.DataNode
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.tools.Logging
import java.util.logging.Level

class Cluster(private val config: Config) {

    private val nodes = HashSet<DataNode>()

    val network = Network()

    fun add(node: DataNode) {
        for (n in nodes) {
            check(n.nodeID != node.nodeID) { "Cluster already contains node $node" }
        }

        nodes.add(node)
        network.connect(node)
    }

    fun getAnyNode(): ClientAPI {
        return nodes.random()
    }

    fun getNode(nodeID: NodeID): ClientAPI {
        for (node in nodes)
            if (nodeID == node.nodeID)
                return node

        throw IllegalArgumentException("Can't find node $nodeID")
    }

    fun start() {
        val logger = Logging.getLogger("raft-cluster")

        logger.log(Level.INFO, "Starting raft cluster")
        logger.log(Level.INFO, "Nodes: " + nodes.map { n -> n.nodeID })
        logger.log(Level.INFO, "Network message delay millis: " + Network.MESSAGE_DELIVERY_DELAY_MILLIS)
        logger.log(Level.INFO, "Network message loss probability: " + Network.MESSAGE_LOSS_PROBABILITY)
        logger.log(Level.INFO, "Network message duplication probability: " + Network.MESSAGE_DUPLICATION_PROBABILITY)
        logger.log(
            Level.INFO,
            "Raft election delay millis: " + config.electionTimeoutMinMs + ".." + config.electionTimeoutMaxMs
        )
        logger.log(Level.INFO, "Raft heartbeat timeout millis: " + config.heartbeatTimeoutMs)

        nodes.forEach { n -> n.start() }

        network.start()
    }
}