package xyz.skywind.raft.cluster

import xyz.skywind.raft.node.Node
import java.util.logging.Level
import java.util.logging.Logger

class Cluster(val config: Config) {

    private val nodes = HashSet<Node>()

    fun add(node: Node) {
        for (n in nodes)
            if (node.nodeID == n.nodeID)
                throw java.lang.IllegalArgumentException("Cluster already contains node $node")

        nodes.add(node)
    }

    fun start() {
        val logger = Logger.getLogger("raft-cluster")
        logger.log(Level.INFO, "Starting raft cluster")
        logger.log(Level.INFO, "Nodes: " + nodes.map { n -> n.nodeID })
        logger.log(Level.INFO, "Network delay millis: " + Network.DEFAULT_NETWORK_DELAY_MILLIS)
        logger.log(Level.INFO, "Raft election delay millis: " + config.electionTimeoutMinMs + " .. " + config.electionTimeoutMaxMs)
        logger.log(Level.INFO, "Raft heartbeat delay millis: " + config.heartbeatTimeoutMs)


        for (node in nodes) {
            node.start()
        }
    }
}