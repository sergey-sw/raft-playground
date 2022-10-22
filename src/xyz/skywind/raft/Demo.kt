package xyz.skywind.raft

import xyz.skywind.raft.cluster.Cluster
import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.node.impl.DataNode
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.tools.Time
import java.util.concurrent.Executors

object Demo {

    @JvmStatic
    fun main(args: Array<String>) {
        val clusterConfig = Config(
                nodeCount = 5,
                electionTimeoutMinMs = Time.millis(150),
                electionTimeoutMaxMs = Time.millis(300),
                heartbeatTimeoutMs = Time.millis(3_000)
        )

        val cluster = Cluster(clusterConfig)
        for (i in 1..clusterConfig.nodeCount) {
            cluster.add(
                    DataNode(NodeID("n$i"), clusterConfig, cluster.network)
            )
        }

        cluster.start()

        val clientsCount = 5
        val threadPool = Executors.newFixedThreadPool(clientsCount)
        for (clientID in 1 .. clientsCount) {
            threadPool.submit(DemoClient(cluster, clientID))
        }
    }
}