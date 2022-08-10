package xyz.skywind.raft

import xyz.skywind.raft.cluster.Cluster
import xyz.skywind.raft.cluster.Config
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

        val clusterConfig = Config(
                nodeCount = 5,
                electionTimeoutMinMs = 150,
                electionTimeoutMaxMs = 300,
                heartbeatTimeoutMs = 3_000
        )

        val cluster = Cluster(clusterConfig)
        for (i in 1..clusterConfig.nodeCount) {
            cluster.add(
                    NodeImpl(NodeID("n$i"), clusterConfig, cluster.network)
            )
        }

        cluster.start()
    }
}