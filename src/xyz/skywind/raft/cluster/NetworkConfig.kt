package xyz.skywind.raft.cluster

data class NetworkConfig(
    val messageDeliveryDelayMillis: Int,
    val messageLossProbability: Float,
    val messageDuplicationProbability: Float,
    val partitionsEnabled: Boolean
) {
    init {
        check(messageDeliveryDelayMillis > 0)

        check(messageLossProbability >= 0)
        check(messageLossProbability < 1)

        check(messageDuplicationProbability >= 0)
        check(messageDuplicationProbability < 1)
    }
}


