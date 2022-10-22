package xyz.skywind.raft.cluster

data class NetworkConfig(
    val messageDeliveryDelayMillis: Int,
    val messageLossProbability: Float,
    val messageDuplicationProbability: Float,
    val partitionsEnabled: Boolean
)
