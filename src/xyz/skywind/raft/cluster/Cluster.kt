package xyz.skywind.raft.cluster

import xyz.skywind.raft.node.Node
import java.util.logging.Level
import java.util.logging.Logger

class Cluster(private val config: Config) {

    private val nodes = HashSet<Node>()

    fun add(node: Node) {
        for (n in nodes) {
            check(n.nodeID != node.nodeID) { "Cluster already contains node $node" }
        }

        nodes.add(node)
    }

    fun start() {
        val logger = Logger.getLogger("raft-cluster")

        logger.log(Level.INFO, "Starting raft cluster")
        logger.log(Level.INFO, "Nodes: " + nodes.map { n -> n.nodeID })
        logger.log(Level.INFO, "Network message delay millis: " + Network.MESSAGE_DELIVERY_DELAY_MILLIS)
        logger.log(Level.INFO, "Network message loss probability: " + Network.MESSAGE_LOSS_PROBABILITY)
        logger.log(Level.INFO, "Network message duplication probability: " + Network.MESSAGE_DUPLICATION_PROBABILITY)
        logger.log(Level.INFO, "Raft election delay millis: " + config.electionTimeoutMinMs + ".." + config.electionTimeoutMaxMs)
        logger.log(Level.INFO, "Raft heartbeat timeout millis: " + config.heartbeatTimeoutMs)

        nodes.forEach { n -> n.start() }
    }
}