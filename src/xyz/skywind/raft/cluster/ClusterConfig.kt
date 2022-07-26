package xyz.skywind.raft.cluster


data class ClusterConfig(val nodeCount: Int,
                         val electionTimeoutMinMs: Long,
                         val electionTimeoutMaxMs: Long,
                         val heartbeatTimeoutMs: Long,
                         val heartbeatTickPeriod: Long = heartbeatTimeoutMs / 4) {

    companion object {
        const val MIN_EL_TIMEOUT = 10 // 10 milli seconds
        const val MAX_EL_TIMEOUT = 10 * 60 * 1000 // 10 minutes
        const val MIN_DELAY_SPREAD = 0 // 50 milli seconds

        const val MAX_HB_TIMEOUT = 1 * 60 * 1000 // 1 minute
    }

    init {
        check(nodeCount > 0) { "Cluster should have nodes" }

        check(electionTimeoutMinMs >= MIN_EL_TIMEOUT) { "Param electionTimeoutMinMs must be >= $MIN_EL_TIMEOUT" }
        check(electionTimeoutMinMs <= MAX_EL_TIMEOUT) { "Param electionTimeoutMinMs must be <= $MAX_EL_TIMEOUT" }

        check(electionTimeoutMaxMs >= MIN_EL_TIMEOUT) { "Param electionTimeoutMaxMs must be >= $MIN_EL_TIMEOUT" }
        check(electionTimeoutMaxMs <= MAX_EL_TIMEOUT) { "Param electionTimeoutMaxMs must be <= $MAX_EL_TIMEOUT" }

        check(electionTimeoutMaxMs - electionTimeoutMinMs >= MIN_DELAY_SPREAD) {
            "Diff between min and max electionTimeoutMs must be >= $MIN_DELAY_SPREAD"
        }

        check(heartbeatTimeoutMs > 0) { "Param heartbeatTimeoutMs must be > 0" }
        check(heartbeatTimeoutMs < MAX_HB_TIMEOUT) { "Param heartbeatTimeoutMs must be <= $MAX_HB_TIMEOUT" }
    }

    fun isQuorum(votes: Int): Boolean {
        return votes >= (nodeCount / 2) + 1
    }
}