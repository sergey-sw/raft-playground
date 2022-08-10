package xyz.skywind.raft.utils

import xyz.skywind.raft.rpc.VoteRequest
import xyz.skywind.raft.rpc.VoteResponse
import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Role
import xyz.skywind.raft.node.State
import xyz.skywind.raft.node.State.*
import xyz.skywind.raft.node.Term
import xyz.skywind.raft.rpc.AppendEntries
import xyz.skywind.raft.rpc.HeartbeatResponse
import xyz.skywind.tools.Time

object States {

    fun initialState(): State {
        return State(
                term = Term(0),
                voteInfo = null,
                role = Role.FOLLOWER,
                leaderInfo = null,
                followers = mapOf()
        )
    }

    fun stepDownToFollowerBecauseOfHigherTerm(state: State, newTerm: Term): State {
        check(state.role != Role.FOLLOWER)

        return State(term = newTerm, voteInfo = null, role = Role.FOLLOWER,
                leaderInfo = null, followers = mapOf())
    }

    fun stepDownToFollowerOnElectionTimeout(state: State): State {
        check(state.role != Role.FOLLOWER)

        return State(term = state.term, voteInfo = state.voteInfo, role = Role.FOLLOWER,
                leaderInfo = null, followers = mapOf())
    }

    fun fromAnyRoleToFollower(msg: AppendEntries): State {
        return State(msg.term, VoteInfo(msg.leader, Time.now()), Role.FOLLOWER, LeaderInfo(msg.leader, Time.now()), mapOf())
    }

    fun becomeCandidate(state: State, nodeID: NodeID): State {
        return State(term = state.term.inc(), voteInfo = VoteInfo(nodeID, Time.now()), role = Role.CANDIDATE,
                leaderInfo = null, followers = mapOf(Pair(nodeID, FollowerInfo(Time.now(), 0))))
    }

    fun stepDownToFollower(msg: VoteRequest): State {
        return State(msg.candidateTerm, voteInfo = VoteInfo(msg.candidate, votedAt = Time.now()), Role.FOLLOWER,
                leaderInfo = null, followers = mapOf())
    }

    fun stepDownToFollower(resp: HeartbeatResponse): State {
        return State(resp.followerTerm, voteInfo = null, Role.FOLLOWER,
                leaderInfo = null, followers = mapOf())
    }

    fun candidateBecomesLeader(state: State, response: VoteResponse): State {
        check(state.role == Role.CANDIDATE)
        check(state.term == response.requestTerm)
        checkNotNull(state.voteInfo) { "Expected to have self vote when receiving VoteResponse"}

        return State(state.term, state.voteInfo, Role.LEADER, LeaderInfo(state.voteInfo.vote, Time.now()), state.followers)
    }

    fun addFollower(state: State, follower: NodeID): State {
        return State(state, followerHeartbeats = state.followers + Pair(follower, FollowerInfo(Time.now(), 0)))
    }

    fun voteFor(state: State, term: Term, candidate: NodeID): State {
        RaftAssertions.verifyNodeDidNotVoteInTerm(state, term, candidate)
        return State(state, term = term, voteInfo = VoteInfo(vote = candidate, votedAt = Time.now()))
    }

    fun updateLeaderHeartbeat(state: State): State {
        checkNotNull(state.leaderInfo)
        return State(state, leader = LeaderInfo(state.leaderInfo.leader, Time.now()))
    }

    fun updateFollowerHeartbeat(state: State, follower: NodeID): State {
        val followers = HashMap(state.followers)
        followers[follower] = FollowerInfo(Time.now(), 0)
        return State(state, followerHeartbeats = followers)
    }
}