package xyz.skywind.raft.node

data class State(
        val term: Term,
        val data: String,
        val role: Role,
        val leader: NodeID?,
        val lastLeaderHeartbeatTs: Long,
        val followers: Set<NodeID>) {

    // second constructor that accepts prev state for default values
    companion object {
        operator fun invoke(s: State,
                            term: Term = s.term,
                            data: String = s.data,
                            role: Role = s.role,
                            leader: NodeID? = s.leader,
                            lastLeaderHeartbeatTs: Long = s.lastLeaderHeartbeatTs,
                            followers: Set<NodeID> = s.followers): State {
            return State(term, data, role, leader, lastLeaderHeartbeatTs, followers)
        }
    }

    init {
        if (role == Role.CANDIDATE) {
            if (followers.isEmpty())
                throw IllegalStateException("Should at least have self as follower when node is a candidate")

            if (term.num < 1)
                throw IllegalStateException("Candidate can't have 0 term")
        }

        if (role == Role.FOLLOWER && followers.isNotEmpty()) {
            throw IllegalStateException("Follower can not have followers")
        }

        if (role == Role.LEADER) {
            if (followers.size <= 1)
                throw IllegalStateException("Leader can not have less than 2 followers (including self)")

            if (term.num < 1)
                throw IllegalStateException("Leader can't be elected on 0 term")
        }
    }
}
