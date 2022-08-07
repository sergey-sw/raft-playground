package xyz.skywind.raft.utils

import xyz.skywind.raft.rpc.LeaderHeartbeat
import xyz.skywind.raft.rpc.VoteRequest
// import xyz.skywind.raft.msg.VoteResponse
import xyz.skywind.raft.rpc.VoteResponse
import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Role
import xyz.skywind.raft.node.State
import xyz.skywind.raft.node.Term
import xyz.skywind.tools.Time

object States {

    fun initialState(): State {
        return State(
                term = Term(0),
                vote = null,
                role = Role.FOLLOWER,
                leader = null,
                lastLeaderHeartbeatTs = 0,
                followerHeartbeats = mapOf()
        )
    }

    fun stepDownToFollower(state: State): State {
        check(state.role != Role.FOLLOWER)

        return State(term = state.term.inc(), vote = null, role = Role.FOLLOWER, leader = null,
                lastLeaderHeartbeatTs = 0, followerHeartbeats = mapOf())
    }

    fun fromAnyRoleToFollower(msg: LeaderHeartbeat): State {
        return State(msg.term, msg.leader, Role.FOLLOWER, msg.leader, Time.now(), mapOf())
    }

    fun becomeCandidate(state: State, nodeID: NodeID): State {
        return State(state, term = state.term.inc(), vote = nodeID, role = Role.CANDIDATE,
                followerHeartbeats = mapOf(Pair(nodeID, Time.now())))
    }

    fun stepDownToFollower(msg: VoteRequest): State {
        return State(msg.candidateTerm, msg.candidate, Role.FOLLOWER, null, 0, mapOf())
    }

    /*fun candidateBecomesLeader(state: State, msg: VoteResponse): State {
        check(state.role == Role.CANDIDATE)
        check(state.term == msg.term)
        check(state.vote == msg.candidate)

        return State(state.term, state.vote, Role.LEADER, state.vote, Time.now(), state.followerHeartbeats)
    }*/

    fun candidateBecomesLeader(state: State, response: VoteResponse): State {
        check(state.role == Role.CANDIDATE)
        check(state.term == response.requestTerm)

        return State(state.term, state.vote, Role.LEADER, state.vote, Time.now(), state.followerHeartbeats)
    }

    fun addFollower(state: State, follower: NodeID): State {
        return State(state, followerHeartbeats = state.followerHeartbeats + Pair(follower, Time.now()))
    }

    fun voteFor(state: State, term: Term, candidate: NodeID): State {
        return State(state, term = term, vote = candidate)
    }

    fun updateLeaderHeartbeat(state: State): State {
        return State(state, lastLeaderHeartbeatTs = Time.now())
    }

    fun updateFollowerHeartbeat(state: State, follower: NodeID): State {
        val followers = HashMap(state.followerHeartbeats)
        followers[follower] = Time.now()
        return State(state, followerHeartbeats = followers)
    }
}