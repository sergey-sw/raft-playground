package xyz.skywind.demo

import xyz.skywind.raft.cluster.Cluster
import xyz.skywind.raft.cluster.ClusterConfig
import xyz.skywind.raft.cluster.NetworkConfig
import xyz.skywind.raft.node.impl.VotingNode
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.tools.Time

/**
 * In this demo we create and start a cluster of ${nodeCount} nodes.
 * Nodes will fight for the leadership. Configure the Network for more drama.
 */
object LeaderElectionDemo {

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
            messageLossProbability = 0.0f,
            messageDuplicationProbability = 0.0f,
            partitionsEnabled = true
        )

        val cluster = Cluster<VotingNode>(clusterConfig, networkConfig)
        for (i in 1..clusterConfig.nodeCount) {
            cluster.add(
                VotingNode(NodeID("n$i"), clusterConfig, cluster.network)
            )
        }

        cluster.start()
    }
}