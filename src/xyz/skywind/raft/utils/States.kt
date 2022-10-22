package xyz.skywind.raft.utils

import xyz.skywind.raft.rpc.VoteRequest
import xyz.skywind.raft.rpc.VoteResponse
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.Role
import xyz.skywind.raft.node.model.State
import xyz.skywind.raft.node.model.State.*
import xyz.skywind.raft.node.model.Term
import xyz.skywind.raft.node.data.OperationLog
import xyz.skywind.raft.node.impl.LastEntryIndex
import xyz.skywind.raft.rpc.AppendEntries
import xyz.skywind.raft.rpc.AppendEntriesResponse
import xyz.skywind.tools.Time
import kotlin.math.min

object States {

    fun initialState(): State {
        return State(
            term = Term(0),
            voteInfo = null,
            role = Role.FOLLOWER,
            commitIdx =  OperationLog.START_IDX,
            appliedIdx = OperationLog.START_IDX,
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
            voteInfo = state.voteInfo, // don't change vote afterwards
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
            followers = mapOf(Pair(nodeID, FollowerInfo(Time.now(), state.commitIdx)))
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

    fun stepDownToFollower(state: State, response: AppendEntriesResponse): State {
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

    fun stepDownAndTryWinAgain(state: State, msg: VoteRequest): State {
        return State(
            term = msg.candidateTerm,
            voteInfo = null,
            role = Role.FOLLOWER,
            leaderInfo = null,
            commitIdx = state.commitIdx,
            appliedIdx = state.appliedIdx,
            followers = mapOf()
        )
    }

    fun candidateBecomesLeader(state: State, lastLogEntryIdx: LastEntryIndex, response: VoteResponse): State {
        check(state.role == Role.CANDIDATE)
        check(state.term == response.requestTerm)
        checkNotNull(state.voteInfo) { "Expected to have self vote when receiving VoteResponse" }

        val followers = HashMap<NodeID, FollowerInfo>()
        for (f in state.followers) {
            followers[f.key] = FollowerInfo(f.value.heartbeatTs, nextIdx = lastLogEntryIdx + 1)
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

    fun addFollower(state: State, lastLogEntryIdx: LastEntryIndex, follower: NodeID): State {
        return updateFollower(state, lastLogEntryIdx, follower)
    }

    fun updateFollower(state: State, followerLastEntryIndex: LastEntryIndex, follower: NodeID): State {
        val followers = HashMap(state.followers)

        followers[follower] = FollowerInfo(Time.now(), nextIdx = followerLastEntryIndex + 1)

        return State(state, followers = followers)
    }

    fun rollbackFollowerIndices(state: State, lastLeaderEntryIdx: LastEntryIndex): State {
        val newFollowers = HashMap<NodeID, FollowerInfo>()
        for (follower in state.followers) {
            val info = follower.value
            newFollowers[follower.key] = FollowerInfo(
                heartbeatTs = info.heartbeatTs,
                nextIdx = min(lastLeaderEntryIdx + 1, info.nextIdx)
            )
        }

        return State(state, followers = newFollowers)
    }

    fun voteFor(state: State, term: Term, candidate: NodeID): State {
        RaftAssertions.verifyNodeDidNotVoteInThisTerm(state, term, candidate)
        return State(state, term = term, voteInfo = VoteInfo(votedFor = candidate, votedAt = Time.now()))
    }

    fun updateLeaderHeartbeatTime(state: State): State {
        checkNotNull(state.leaderInfo)
        return State(state, leader = LeaderInfo(state.leaderInfo.leader, Time.now()))
    }

    fun updateIndices(state: State, index: Int): State {
        return State(
            state,
            commitIdx = index,
            appliedIdx = index
        )
    }

    fun incCommitAndAppliedIndices(state: State, cnt: Int): State {
        return State(
            state,
            commitIdx = state.commitIdx + cnt,
            appliedIdx = state.appliedIdx + cnt
        )
    }
}