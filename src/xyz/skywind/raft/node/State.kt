package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.tools.Time

data class State(
        val term: Term,
        val voteInfo: VoteInfo?,
        val role: Role,
        val leaderInfo: LeaderInfo?,
        val followerHeartbeats: Map<NodeID, Long>) {

    data class LeaderInfo(val leader: NodeID, val lastHeartbeatTs: Long)
    data class VoteInfo(val vote: NodeID, val votedAt: Long)

    // second constructor that accepts prev state for default values
    companion object {
        operator fun invoke(s: State,
                            term: Term = s.term,
                            voteInfo: VoteInfo? = s.voteInfo,
                            role: Role = s.role,
                            leader: LeaderInfo? = s.leaderInfo,
                            followerHeartbeats: Map<NodeID, Long> = s.followerHeartbeats): State {
            return State(term, voteInfo, role, leader, followerHeartbeats)
        }
    }

    init {
        if (role == Role.CANDIDATE) {
            check(followerHeartbeats.isNotEmpty()) { "Should at least have self as follower when node is a candidate" }
            check(term.num > 0) { "Candidate can't have 0 term" }
            check(leaderInfo == null) { "Candidate can't have active leader" }
            checkNotNull(voteInfo) { "Candidate should vote for itself" }
            check(followerHeartbeats.contains(voteInfo.vote)) { "Candidate should have a vote for itself" }
        }

        if (role == Role.FOLLOWER) {
            check(followerHeartbeats.isEmpty()) { "Follower can not have followers" }
        }

        if (role == Role.LEADER) {
            check(term.num > 0) { "Leader can't be elected on 0 term" }
            check(followerHeartbeats.size > 1) { "Leader can not have less than 2 followers (including self)" }
            checkNotNull(voteInfo) { "Leader should have a vote for itself" }
            checkNotNull(leaderInfo) { "Leader should have leader property set" }
            check(voteInfo.vote == leaderInfo.leader) { "Chosen leader should vote for itself" }
            check(followerHeartbeats.contains(voteInfo.vote)) { "Candidate should have a vote for itself" }
        }
    }

    fun votedInThisTerm(t: Term): Boolean {
        return this.term.num == t.num && voteInfo != null
    }

    fun votedFor(candidate: NodeID): Boolean {
        return voteInfo != null && voteInfo.vote == candidate
    }

    fun canAcceptTerm(t: Term): Boolean {
        return this.term.num <= t.num
    }

    fun needSelfPromotion(cfg: Config): Boolean {
        if (role != Role.FOLLOWER) {
            return false
        }

        val hasRecentVote = voteInfo != null && (Time.now() - voteInfo.votedAt) <= cfg.electionTimeoutMinMs
        if (hasRecentVote) {
            return false
        }

        val noLeaderInCluster = leaderInfo == null
        val noLeaderHeartbeat = leaderInfo != null && Time.now() - leaderInfo.lastHeartbeatTs > cfg.heartbeatTimeoutMs

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
        follower2delay.remove(leaderInfo?.leader)

        return follower2delay
    }
}
