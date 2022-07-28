package xyz.skywind.raft.cluster

data class Config (val nodeCount: Int,
                   val electionTimeoutMinMs: Int,
                   val electionTimeoutMaxMs: Int,
                   val heartbeatTimeoutMs: Int) {

    fun isQuorum(votes: Int) : Boolean {
        return votes >= 1 + nodeCount / 2
    }
}