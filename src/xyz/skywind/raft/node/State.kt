package xyz.skywind.raft.node

data class State(
        val term: Term,
        val vote: NodeID?,
        val role: Role,
        val leader: NodeID?,
        val lastLeaderHeartbeatTs: Long,
        val followers: Set<NodeID>) {

    // second constructor that accepts prev state for default values
    companion object {
        operator fun invoke(s: State,
                            term: Term = s.term,
                            vote: NodeID? = s.vote,
                            role: Role = s.role,
                            leader: NodeID? = s.leader,
                            lastLeaderHeartbeatTs: Long = s.lastLeaderHeartbeatTs,
                            followers: Set<NodeID> = s.followers): State {
            return State(term, vote, role, leader, lastLeaderHeartbeatTs, followers)
        }
    }

    init {
        if (role == Role.CANDIDATE) {
            check(followers.isNotEmpty()) { "Should at least have self as follower when node is a candidate" }
            check(term.num > 0) { "Candidate can't have 0 term" }
            checkNotNull(vote) { "Candidate should vote for itself" }
            check(followers.contains(vote)) { "Candidate should have a vote for itself" }
        }

        if (role == Role.FOLLOWER) {
            check(followers.isEmpty()) { "Follower can not have followers" }
        }

        if (role == Role.LEADER) {
            check(term.num > 0) { "Leader can't be elected on 0 term" }
            check(followers.size > 1) { "Leader can not have less than 2 followers (including self)" }
            checkNotNull(vote) { "Leader should have a vote for itself" }
            check(followers.contains(vote)) { "Candidate should have a vote for itself" }
        }
    }

    fun votedInThisTerm(t: Term): Boolean {
        return this.term.num == t.num && vote != null
    }

    fun canAcceptTerm(t: Term) : Boolean {
        return this.term.num <= t.num
    }
}
