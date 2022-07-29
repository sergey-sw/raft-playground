package xyz.skywind.raft.cluster


data class Config(val nodeCount: Int,
                  val electionTimeoutMinMs: Int,
                  val electionTimeoutMaxMs: Int,
                  val heartbeatTimeoutMs: Int) {

    companion object {
        const val MIN_TIMEOUT = 10 // 10 milli seconds
        const val MAX_TIMEOUT = 10 * 60 * 1000 // 10 minutes
        const val MIN_DELAY_SPREAD = 0 // 50 milli seconds
    }

    init {
        check(nodeCount > 0) { "Cluster should have nodes" }

        check(electionTimeoutMinMs > MIN_TIMEOUT) { "Param electionTimeoutMinMs must be >= $MIN_TIMEOUT" }
        check(electionTimeoutMinMs < MAX_TIMEOUT) { "Param electionTimeoutMinMs must be <= $MAX_TIMEOUT" }

        check(electionTimeoutMaxMs > MIN_TIMEOUT) { "Param electionTimeoutMaxMs must be >= $MIN_TIMEOUT" }
        check(electionTimeoutMaxMs < MAX_TIMEOUT) { "Param electionTimeoutMaxMs must be <= $MAX_TIMEOUT" }

        check(electionTimeoutMaxMs - electionTimeoutMinMs >= MIN_DELAY_SPREAD) {
            "Diff between min and max electionTimeoutMs must be >= $MIN_DELAY_SPREAD "
        }

        check(heartbeatTimeoutMs > MIN_TIMEOUT) { "Param heartbeatTimeoutMs must be >= $MIN_TIMEOUT" }
        check(heartbeatTimeoutMs < MAX_TIMEOUT) { "Param heartbeatTimeoutMs must be <= $MAX_TIMEOUT" }
    }

    fun isQuorum(votes: Int): Boolean {
        return votes >= 1 + nodeCount / 2
    }
}