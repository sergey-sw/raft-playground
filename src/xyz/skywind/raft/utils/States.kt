package xyz.skywind.raft.utils

import xyz.skywind.raft.msg.NewLeaderMessage
import xyz.skywind.raft.msg.VoteRequest
import xyz.skywind.raft.msg.VoteResponse
import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Role
import xyz.skywind.raft.node.State
import xyz.skywind.raft.node.Term
import xyz.skywind.tools.Time

object States {

    fun fromCandidateToFollower(state: State): State {
        check(state.role == Role.CANDIDATE)

        return State(term = state.term.inc(), vote = null, role = Role.FOLLOWER, leader = null,
                lastLeaderHeartbeatTs = 0, followers = setOf())
    }

    fun fromAnyRoleToFollower(msg: NewLeaderMessage): State {
        return State(msg.term, msg.leader, Role.FOLLOWER, msg.leader, Time.now(), setOf())
    }

    fun becomeCandidate(state: State, nodeID: NodeID): State {
        return State(state, term = state.term.inc(), vote = nodeID, role = Role.CANDIDATE, followers = setOf(nodeID))
    }

    fun stepDownToFollower(msg: VoteRequest): State {
        return State(msg.term, msg.candidate, Role.FOLLOWER, null, 0, setOf())
    }

    fun candidateBecomesLeader(state: State, msg: VoteResponse): State {
        check(state.role == Role.CANDIDATE)
        check(state.term == msg.term)
        check(state.vote == msg.candidate)

        return State(state.term, state.vote, Role.LEADER, state.vote, Time.now(), state.followers)
    }

    fun addFollower(state: State, follower: NodeID): State {
        return State(state, followers = state.followers + follower)
    }

    fun voteFor(state: State, term: Term, candidate: NodeID): State {
        return State(state, term = term, vote = candidate)
    }
}