package xyz.skywind.raft.node.model

import xyz.skywind.raft.cluster.Config
import xyz.skywind.tools.Time
import xyz.skywind.tools.Timestamp

data class State(
    val term: Term,
    val voteInfo: VoteInfo?,
    val role: Role,
    val leaderInfo: LeaderInfo?,
    // TODO create value types for commit/applied index
    val commitIdx: Int, // index of the highest log entry known to be committed
    val appliedIdx: Int, // index of the highest log entry applied to the state machine
    val followers: Map<NodeID, FollowerInfo>
) {

    data class LeaderInfo(val leader: NodeID, val lastHeartbeatTs: Timestamp)
    data class VoteInfo(val votedFor: NodeID, val votedAt: Timestamp)

    // nextIdx: index of next log entry that should be sent to follower
    // matchIdx: index of max entry that matches with leader
    data class FollowerInfo(val heartbeatTs: Timestamp, val nextIdx: Int, val matchIdx: Int)

    // second constructor that accepts prev state for default values
    companion object {
        operator fun invoke(
            s: State,
            term: Term = s.term,
            voteInfo: VoteInfo? = s.voteInfo,
            role: Role = s.role,
            leader: LeaderInfo? = s.leaderInfo,
            commitIdx: Int = s.commitIdx,
            appliedIdx: Int = s.appliedIdx,
            followers: Map<NodeID, FollowerInfo> = s.followers
        ): State {
            return State(term, voteInfo, role, leader, commitIdx, appliedIdx, followers)
        }
    }

    init { // checking invariants
        if (role == Role.CANDIDATE) {
            check(term.num > 0) { "Candidate can't have 0 term" }
            check(leaderInfo == null) { "Candidate can't have active leader" }
            checkNotNull(voteInfo) { "Candidate should vote for itself" }
            check(followers.contains(voteInfo.votedFor)) { "Candidate should have a vote for itself" }
        }

        if (role == Role.FOLLOWER) {
            check(followers.isEmpty()) { "Follower can not have followers" }
        }

        if (role == Role.LEADER) {
            check(term.num > 0) { "Leader can't be elected on 0 term" }
            check(followers.size > 1) { "Leader can not have less than 2 followers (including self)" }
            checkNotNull(voteInfo) { "Leader should have a vote for itself" }
            checkNotNull(leaderInfo) { "Leader should have leader property set" }
            check(voteInfo.votedFor == leaderInfo.leader) { "Chosen leader should vote for itself" }
            check(followers.contains(voteInfo.votedFor)) { "Candidate should have a vote for itself" }
        }
    }

    fun votedInThisTerm(t: Term): Boolean {
        return this.term.num == t.num && voteInfo != null
    }

    fun votedFor(candidate: NodeID): Boolean {
        return voteInfo != null && voteInfo.votedFor == candidate
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
