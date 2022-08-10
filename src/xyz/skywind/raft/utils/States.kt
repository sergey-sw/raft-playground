package xyz.skywind.raft.utils

import xyz.skywind.raft.rpc.LeaderHeartbeat
import xyz.skywind.raft.rpc.VoteRequest
import xyz.skywind.raft.rpc.VoteResponse
import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Role
import xyz.skywind.raft.node.State
import xyz.skywind.raft.node.State.LeaderInfo
import xyz.skywind.raft.node.Term
import xyz.skywind.raft.rpc.HeartbeatResponse
import xyz.skywind.tools.Time

object States {

    fun initialState(): State {
        return State(
                term = Term(0),
                vote = null,
                votedAt = null,
                role = Role.FOLLOWER,
                leaderInfo = null,
                followerHeartbeats = mapOf()
        )
    }

    fun stepDownToFollowerBecauseOfHigherTerm(state: State, newTerm: Term): State {
        check(state.role != Role.FOLLOWER)

        return State(term = newTerm, vote = null, votedAt = null, role = Role.FOLLOWER,
                leaderInfo = null, followerHeartbeats = mapOf())
    }

    fun stepDownToFollowerOnElectionTimeout(state: State): State {
        check(state.role != Role.FOLLOWER)

        return State(term = state.term, vote = state.vote, votedAt = state.votedAt, role = Role.FOLLOWER,
                leaderInfo = null, followerHeartbeats = mapOf())
    }

    fun fromAnyRoleToFollower(msg: LeaderHeartbeat): State {
        return State(msg.term, msg.leader, Time.now(), Role.FOLLOWER, LeaderInfo(msg.leader, Time.now()), mapOf())
    }

    fun becomeCandidate(state: State, nodeID: NodeID): State {
        return State(term = state.term.inc(), vote = nodeID, votedAt = Time.now(), role = Role.CANDIDATE,
                leaderInfo = null, followerHeartbeats = mapOf(Pair(nodeID, Time.now())))
    }

    fun stepDownToFollower(msg: VoteRequest): State {
        return State(msg.candidateTerm, vote = msg.candidate, votedAt = Time.now(), Role.FOLLOWER,
                leaderInfo = null, followerHeartbeats = mapOf())
    }

    fun stepDownToFollower(resp: HeartbeatResponse): State {
        return State(resp.followerTerm, vote = null, votedAt = null, Role.FOLLOWER,
                leaderInfo = null, followerHeartbeats = mapOf())
    }

    fun candidateBecomesLeader(state: State, response: VoteResponse): State {
        check(state.role == Role.CANDIDATE)
        check(state.term == response.requestTerm)
        checkNotNull(state.vote) { "Expected to have self vote when receiving VoteResponse"}

        return State(state.term, state.vote, state.votedAt, Role.LEADER, LeaderInfo(state.vote, Time.now()), state.followerHeartbeats)
    }

    fun addFollower(state: State, follower: NodeID): State {
        return State(state, followerHeartbeats = state.followerHeartbeats + Pair(follower, Time.now()))
    }

    fun voteFor(state: State, term: Term, candidate: NodeID): State {
        RaftAssertions.verifyNodeDidNotVoteInTerm(state, term, candidate)
        return State(state, term = term, vote = candidate, votedAt = Time.now())
    }

    fun updateLeaderHeartbeat(state: State): State {
        checkNotNull(state.leaderInfo)
        return State(state, leader = LeaderInfo(state.leaderInfo.leader, Time.now()))
    }

    fun updateFollowerHeartbeat(state: State, follower: NodeID): State {
        val followers = HashMap(state.followerHeartbeats)
        followers[follower] = Time.now()
        return State(state, followerHeartbeats = followers)
    }
}