package xyz.skywind.raft

import xyz.skywind.raft.cluster.Cluster
import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.cluster.Network
import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.NodeImpl

object Demo {

    @JvmStatic
    fun main(args: Array<String>) {
        // setup logging
        System.setProperty(
                "java.util.logging.SimpleFormatter.format",
                "%1\$tF %1\$tT.%1\$tL %3\$s %4\$s %5\$s%6\$s%n"
        )

        val network = Network()

        val config = Config(
                nodeCount = 5,
                electionTimeoutMinMs = 150,
                electionTimeoutMaxMs = 300,
                heartbeatTimeoutMs = 3_000
        )

        val cluster = Cluster(config)
        for (i in 1..config.nodeCount) {
            val node = NodeImpl(NodeID("n$i"), config, network)

            cluster.add(node)
            network.connect(node)
        }

        cluster.start()
    }
}