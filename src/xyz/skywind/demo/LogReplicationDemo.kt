package xyz.skywind.demo

import xyz.skywind.raft.cluster.Cluster
import xyz.skywind.raft.cluster.ClusterConfig
import xyz.skywind.raft.cluster.NetworkConfig
import xyz.skywind.raft.node.impl.DataNode
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.tools.Time
import java.util.concurrent.Executors

/**
 * In this demo we create a cluster of ${nodeCount} data nodes and some clients that will access it.
 * DataNode implements ClientAPI: it allows to get/set/remove values by keys.
 *
 * Clients pick a random operation and execute it on the cluster in an endless loop. Clients maintain a shared
 * key-value storage and verify that its content matches raft cluster content after every operation. If data in a
 * local key-value storage does not match data in a raft-cluster, then algorithm does not work properly.
 */
object LogReplicationDemo {

    @JvmStatic
    fun main(args: Array<String>) {
        val clusterConfig = ClusterConfig(
            nodeCount = 5,
            electionTimeoutMinMs = Time.millis(150),
            electionTimeoutMaxMs = Time.millis(300),
            heartbeatTimeoutMs = Time.millis(3_000)
        )
        val networkConfig = NetworkConfig(
            messageDeliveryDelayMillis = 5,
            messageLossProbability = 0.02f,
            messageDuplicationProbability = 0.05f,
            partitionsEnabled = true
        )

        val cluster = Cluster<DataNode>(clusterConfig, networkConfig)
        for (i in 1..clusterConfig.nodeCount) {
            cluster.add(
                DataNode(NodeID("n$i"), clusterConfig, cluster.network)
            )
        }

        cluster.start()

        val clientsCount = 5
        val threadPool = Executors.newFixedThreadPool(clientsCount)
        for (clientID in 1..clientsCount) {
            threadPool.submit(LogReplicationDemoClient(cluster, clientID))
        }
    }
}