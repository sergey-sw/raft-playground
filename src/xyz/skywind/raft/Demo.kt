package xyz.skywind.raft

import xyz.skywind.raft.cluster.Cluster
import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.node.impl.DataNode
import xyz.skywind.raft.node.model.NodeID
import java.util.concurrent.Executors

object Demo {

    @JvmStatic
    fun main(args: Array<String>) {
        val clusterConfig = Config(
                nodeCount = 5,
                electionTimeoutMinMs = 150,
                electionTimeoutMaxMs = 300,
                heartbeatTimeoutMs = 3_000
        )

        val cluster = Cluster(clusterConfig)
        for (i in 1..clusterConfig.nodeCount) {
            cluster.add(
                    DataNode(NodeID("n$i"), clusterConfig, cluster.network)
            )
        }

        cluster.start()

        val clientsCount = 1
        val threadPool = Executors.newFixedThreadPool(clientsCount)
        for (i in 1 .. clientsCount) {
            threadPool.submit(DemoClient(cluster, i))
        }
    }
}