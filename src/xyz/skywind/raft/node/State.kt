package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.tools.Time

data class State(
        val term: Term,
        val vote: NodeID?,
        val role: Role,
        val leader: NodeID?,
        val lastLeaderHeartbeatTs: Long,
        val followerHeartbeats: Map<NodeID, Long>) {

    // second constructor that accepts prev state for default values
    companion object {
        operator fun invoke(s: State,
                            term: Term = s.term,
                            vote: NodeID? = s.vote,
                            role: Role = s.role,
                            leader: NodeID? = s.leader,
                            lastLeaderHeartbeatTs: Long = s.lastLeaderHeartbeatTs,
                            followerHeartbeats: Map<NodeID, Long> = s.followerHeartbeats): State {
            return State(term, vote, role, leader, lastLeaderHeartbeatTs, followerHeartbeats)
        }
    }

    init {
        if (role == Role.CANDIDATE) {
            check(followerHeartbeats.isNotEmpty()) { "Should at least have self as follower when node is a candidate" }
            check(term.num > 0) { "Candidate can't have 0 term" }
            checkNotNull(vote) { "Candidate should vote for itself" }
            check(followerHeartbeats.contains(vote)) { "Candidate should have a vote for itself" }
        }

        if (role == Role.FOLLOWER) {
            check(followerHeartbeats.isEmpty()) { "Follower can not have followers" }
        }

        if (role == Role.LEADER) {
            check(term.num > 0) { "Leader can't be elected on 0 term" }
            check(followerHeartbeats.size > 1) { "Leader can not have less than 2 followers (including self)" }
            checkNotNull(vote) { "Leader should have a vote for itself" }
            check(followerHeartbeats.contains(vote)) { "Candidate should have a vote for itself" }
        }
    }

    fun votedInThisTerm(t: Term): Boolean {
        return this.term.num == t.num && vote != null
    }

    fun canAcceptTerm(t: Term): Boolean {
        return this.term.num <= t.num
    }

    fun needSelfPromotion(cfg: Config): Boolean {
        val noLeaderInCluster = leader == null
        val noLeaderHeartbeat = Time.now() - lastLeaderHeartbeatTs > cfg.heartbeatTimeoutMs

        return noLeaderInCluster || noLeaderHeartbeat
    }

    fun followers(): Set<NodeID> {
        return followerHeartbeats.keys
    }

    fun lastResponseFromFollowers(): HashMap<NodeID, Long> {
        val follower2delay = HashMap<NodeID, Long>()

        for (followerHeartbeat in followerHeartbeats) {
            follower2delay[followerHeartbeat.key] = Time.now() - followerHeartbeat.value
        }
        follower2delay.remove(leader)

        return follower2delay
    }
}
