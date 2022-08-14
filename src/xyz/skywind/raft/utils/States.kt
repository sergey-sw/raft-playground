package xyz.skywind.raft.utils

import xyz.skywind.raft.rpc.VoteRequest
import xyz.skywind.raft.rpc.VoteResponse
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.Role
import xyz.skywind.raft.node.model.State
import xyz.skywind.raft.node.model.State.*
import xyz.skywind.raft.node.model.Term
import xyz.skywind.raft.node.data.LogEntryInfo
import xyz.skywind.raft.rpc.AppendEntries
import xyz.skywind.raft.rpc.HeartbeatResponse
import xyz.skywind.tools.Time

object States {

    fun initialState(): State {
        return State(
            term = Term(0),
            voteInfo = null,
            role = Role.FOLLOWER,
            commitIdx = -1,
            appliedIdx = -1,
            leaderInfo = null,
            followers = mapOf()
        )
    }

    fun stepDownToFollowerBecauseOfHigherTerm(state: State, newTerm: Term): State {
        check(state.role != Role.FOLLOWER)

        return State(
            term = newTerm,
            voteInfo = null,
            role = Role.FOLLOWER,
            leaderInfo = null,
            commitIdx = state.commitIdx,
            appliedIdx = state.appliedIdx,
            followers = mapOf()
        )
    }

    fun stepDownToFollowerOnElectionTimeout(state: State): State {
        check(state.role != Role.FOLLOWER)

        return State(
            term = state.term,
            voteInfo = state.voteInfo,
            role = Role.FOLLOWER,
            leaderInfo = null,
            commitIdx = state.commitIdx,
            appliedIdx = state.appliedIdx,
            followers = mapOf()
        )
    }

    fun fromAnyRoleToFollower(state: State, msg: AppendEntries): State {
        return State(
            term = msg.term,
            voteInfo = VoteInfo(msg.leader, Time.now()),
            role = Role.FOLLOWER,
            leaderInfo = LeaderInfo(msg.leader, Time.now()),
            commitIdx = state.commitIdx,
            appliedIdx = state.appliedIdx,
            followers = mapOf()
        )
    }

    fun becomeCandidate(state: State, nodeID: NodeID): State {
        return State(
            term = state.term.inc(),
            voteInfo = VoteInfo(nodeID, Time.now()),
            role = Role.CANDIDATE,
            leaderInfo = null,
            commitIdx = state.commitIdx,
            appliedIdx = state.appliedIdx,
            followers = mapOf(Pair(nodeID, FollowerInfo(Time.now(), state.commitIdx, 0)))
        )
    }

    fun stepDownToFollower(state: State, msg: VoteRequest): State {
        return State(
            term = msg.candidateTerm,
            voteInfo = VoteInfo(msg.candidate, votedAt = Time.now()),
            role = Role.FOLLOWER,
            leaderInfo = null,
            commitIdx = state.commitIdx,
            appliedIdx = state.appliedIdx,
            followers = mapOf()
        )
    }

    fun stepDownToFollower(state: State, response: HeartbeatResponse): State {
        return State(
            term = response.followerTerm,
            voteInfo = null,
            role = Role.FOLLOWER,
            leaderInfo = null,
            commitIdx = state.commitIdx,
            appliedIdx = state.appliedIdx,
            followers = mapOf()
        )
    }

    fun candidateBecomesLeader(state: State, prevLogEntry: LogEntryInfo, response: VoteResponse): State {
        check(state.role == Role.CANDIDATE)
        check(state.term == response.requestTerm)
        checkNotNull(state.voteInfo) { "Expected to have self vote when receiving VoteResponse" }

        val followers = HashMap<NodeID, FollowerInfo>()
        for (f in state.followers) {
            followers[f.key] = FollowerInfo(f.value.heartbeatTs, nextIdx = prevLogEntry.index, matchIdx = 0)
        }

        return State(
            term = state.term,
            voteInfo = state.voteInfo,
            role = Role.LEADER,
            leaderInfo = LeaderInfo(state.voteInfo.votedFor, Time.now()),
            commitIdx = state.commitIdx,
            appliedIdx = state.appliedIdx,
            followers = followers
        )
    }

    fun addFollower(state: State, follower: NodeID): State {
        val followers = HashMap(state.followers)

        val prevFollowerInfo = followers[follower]
        if (prevFollowerInfo != null) {
            followers[follower] = FollowerInfo(Time.now(), prevFollowerInfo.nextIdx, prevFollowerInfo.matchIdx)
        } else {
            followers[follower] = FollowerInfo(Time.now(), nextIdx = state.commitIdx, matchIdx = 0)
        }

        return State(state, followers = followers)
    }

    fun voteFor(state: State, term: Term, candidate: NodeID): State {
        RaftAssertions.verifyNodeDidNotVoteInTerm(state, term, candidate)
        return State(state, term = term, voteInfo = VoteInfo(votedFor = candidate, votedAt = Time.now()))
    }

    fun updateLeaderHeartbeat(state: State): State {
        checkNotNull(state.leaderInfo)
        return State(state, leader = LeaderInfo(state.leaderInfo.leader, Time.now()))
    }

    fun incCommitIndex(state: State): State {
        return State(state, commitIdx = state.commitIdx + 1)
    }

    fun incAppliedIndex(state: State): State {
        return State(state, appliedIdx = state.appliedIdx + 1)
    }
}