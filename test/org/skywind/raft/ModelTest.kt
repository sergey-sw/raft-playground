package org.skywind.raft

import xyz.skywind.raft.msg.VoteResponse
import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Role
import xyz.skywind.raft.node.State
import xyz.skywind.raft.node.Term
import xyz.skywind.tools.Time

object ModelTest {

    @JvmStatic
    fun main(args: Array<String>) {
        voteResponseValidation()
        termValidation()
        termComparator()
        testState()
    }

    private fun voteResponseValidation() {
        VoteResponse(NodeID("1"), NodeID("2"), Term(1))

        try {
            VoteResponse(NodeID("1"), NodeID("1"), Term(1))
            throw AssertionError("Expected to fail on same arguments")
        } catch (e: IllegalArgumentException) {
            return
        }
    }

    private fun termValidation() {
        Term(0)
        Term(1)
        Term(Int.MAX_VALUE.toLong())

        try {
            Term(-42)
            throw AssertionError("Expected to fail on negative term values")
        } catch (e: IllegalArgumentException) {
            return
        }
    }

    private fun termComparator() {
        val t1 = Term(3)
        val t2 = Term(5)
        val t3 = Term(5)

        if (t1 < t2 && t1 <= t2 && t2 > t1 && t2 >= t1 && t2 == t3)
            return

        throw AssertionError("Term comparator does not work")
    }

    private fun testState() {
        State(Term(1), null, Role.FOLLOWER, null, 0, setOf())
        State(Term(1), NodeID("1"), Role.CANDIDATE, null, 0, setOf(NodeID("1")))
        State(Term(1), NodeID("1"), Role.LEADER, null, 0, setOf(NodeID("1"), NodeID("2"), NodeID("3")))

        testFollowerHasFollowers()
        testCandidateFollowsSelf()
        testLeaderFollowsSelf()
        testLeaderIsFollowed()

        testCandidateVotesSelf()
        testLeaderVotesSelf()

        testLeaderHasCorrectTerm()
        testCandidateHasCorrectTerm()
        testFollowerIsAllowedZeroTerm()

        testStateCopy()
    }

    private fun testFollowerHasFollowers() {
        try {
            State(Term(1), null, Role.FOLLOWER, null, 0, setOf(NodeID("1")))
            throw AssertionError("Expected to fail if follower has followers")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testCandidateFollowsSelf() {
        try {
            State(Term(1), null, Role.CANDIDATE, null, 0, setOf())
            throw AssertionError("Expected to fail if candidate does not follow self")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testCandidateVotesSelf() {
        try {
            State(Term(1), null, Role.CANDIDATE, null, 0, setOf(NodeID("123")))
            throw AssertionError("Expected to fail if candidate does not vote for itself")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testLeaderFollowsSelf() {
        try {
            State(Term(1), null, Role.LEADER, null, 0, setOf())
            throw AssertionError("Expected to fail if leader does not follow self")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testLeaderIsFollowed() {
        try {
            State(Term(1), null, Role.LEADER, null, 0, setOf(NodeID("1")))
            throw AssertionError("Expected to fail if leader does not have at least 2 followers")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testLeaderVotesSelf() {
        try {
            State(Term(1), null, Role.CANDIDATE, null, 0, setOf(NodeID("1"), NodeID("2")))
            throw AssertionError("Expected to fail if leader does not vote for itself")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testLeaderHasCorrectTerm() {
        try {
            State(Term(0), null, Role.LEADER, null, 0, setOf(NodeID("123")))
            throw AssertionError("Expected to fail if leader has term equal 0")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testCandidateHasCorrectTerm() {
        try {
            State(Term(0), null, Role.CANDIDATE, null, 0, setOf(NodeID("123")))
            throw AssertionError("Expected to fail if candidate has term equal 0")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testFollowerIsAllowedZeroTerm() {
        State(Term(0), null, Role.FOLLOWER, null, 0, setOf())
    }

    private fun testStateCopy() {
        val state = State(Term(0), null, Role.FOLLOWER, NodeID("3"), Time.now(), setOf())

        val copyTerm = Term(5)
        val copy = State(state, copyTerm)

        if (copy.term != copyTerm || copy.role != state.role
                || copy.leader != state.leader || copy.lastLeaderHeartbeatTs != state.lastLeaderHeartbeatTs
                || copy.followers != state.followers) {
            throw AssertionError("State 'copy' constructor does not work")
        }
    }
}