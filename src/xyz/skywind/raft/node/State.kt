package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.tools.Time
import xyz.skywind.tools.Timestamp

data class State(
        val term: Term,
        val voteInfo: VoteInfo?,
        val role: Role,
        val leaderInfo: LeaderInfo?,
        val followers: Map<NodeID, FollowerInfo>) {

    data class LeaderInfo(val leader: NodeID, val lastHeartbeatTs: Timestamp)
    data class VoteInfo(val vote: NodeID, val votedAt: Timestamp)
    data class FollowerInfo(val heartbeatTs: Timestamp, val nextIndex: Int)

    // second constructor that accepts prev state for default values
    companion object {
        operator fun invoke(s: State,
                            term: Term = s.term,
                            voteInfo: VoteInfo? = s.voteInfo,
                            role: Role = s.role,
                            leader: LeaderInfo? = s.leaderInfo,
                            followerHeartbeats: Map<NodeID, FollowerInfo> = s.followers): State {
            return State(term, voteInfo, role, leader, followerHeartbeats)
        }
    }

    init {
        if (role == Role.CANDIDATE) {
            check(followers.isNotEmpty()) { "Should at least have self as follower when node is a candidate" }
            check(term.num > 0) { "Candidate can't have 0 term" }
            check(leaderInfo == null) { "Candidate can't have active leader" }
            checkNotNull(voteInfo) { "Candidate should vote for itself" }
            check(followers.contains(voteInfo.vote)) { "Candidate should have a vote for itself" }
        }

        if (role == Role.FOLLOWER) {
            check(followers.isEmpty()) { "Follower can not have followers" }
        }

        if (role == Role.LEADER) {
            check(term.num > 0) { "Leader can't be elected on 0 term" }
            check(followers.size > 1) { "Leader can not have less than 2 followers (including self)" }
            checkNotNull(voteInfo) { "Leader should have a vote for itself" }
            checkNotNull(leaderInfo) { "Leader should have leader property set" }
            check(voteInfo.vote == leaderInfo.leader) { "Chosen leader should vote for itself" }
            check(followers.contains(voteInfo.vote)) { "Candidate should have a vote for itself" }
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
        return followers.keys
    }

    fun lastResponseFromFollowers(): HashMap<NodeID, Long> {
        val follower2delay = HashMap<NodeID, Long>()

        for (follower in followers) {
            follower2delay[follower.key] = Time.now() - follower.value.heartbeatTs
        }
        follower2delay.remove(leaderInfo?.leader)

        return follower2delay
    }
}
