package org.skywind.raft

// import xyz.skywind.raft.msg.VoteResponse
import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Role
import xyz.skywind.raft.node.State
import xyz.skywind.raft.node.Term
import xyz.skywind.tools.Time

object ModelTest {

    @JvmStatic
    fun main(args: Array<String>) {
        // voteResponseValidation()
        termValidation()
        termComparator()
        testState()

        println("Tests passed")
    }

    /*private fun voteResponseValidation() {
        VoteResponse(NodeID("1"), NodeID("2"), Term(1))

        try {
            VoteResponse(NodeID("1"), NodeID("1"), Term(1))
            throw AssertionError("Expected to fail on same arguments")
        } catch (e: IllegalArgumentException) {
            return
        }
    }*/

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
        State(
                term = Term(1),
                vote = null,
                role = Role.FOLLOWER,
                leader = null,
                lastLeaderHeartbeatTs = 0,
                mapOf()
        )

        State(
                term = Term(1),
                vote = NodeID("1"),
                role = Role.CANDIDATE,
                leader = null,
                lastLeaderHeartbeatTs = 0,
                followerHeartbeats = mapOf(Pair(NodeID("1"), Time.now()))
        )

        State(
                term = Term(1),
                vote = NodeID("1"),
                role = Role.LEADER,
                leader = null,
                lastLeaderHeartbeatTs = 0,
                followerHeartbeats = mapOf(
                        Pair(NodeID("1"), Time.now()),
                        Pair(NodeID("2"), Time.now()),
                        Pair(NodeID("3"), Time.now())
                )
        )

        testFollowerShouldNotHaveFollowers()
        testCandidateShouldFollowSelf()
        testLeaderShouldFollowSelf()
        testLeaderShouldBeFollowed()

        testCandidateShouldVoteSelf()
        testLeaderShouldVoteForSelf()

        testLeaderHasCorrectTerm()
        testCandidateHasCorrectTerm()
        testFollowerIsAllowedZeroTerm()

        testStateCopy()
    }

    private fun testOkFollowerStates() {
        State(
                term = Term(1),
                vote = null,
                role = Role.FOLLOWER,
                leader = null,
                lastLeaderHeartbeatTs = 0,
                mapOf()
        )

        State(
                term = Term(1),
                vote = NodeID("123"),
                role = Role.FOLLOWER,
                leader = null,
                lastLeaderHeartbeatTs = 0,
                mapOf()
        )

        State(
                term = Term(1),
                vote = NodeID("123"),
                role = Role.FOLLOWER,
                leader = NodeID("123"),
                lastLeaderHeartbeatTs = 0,
                mapOf()
        )
    }

    private fun testFollowerShouldNotHaveFollowers() {
        try {
            State(
                    term = Term(1),
                    vote = null,
                    role = Role.FOLLOWER,
                    leader = null,
                    lastLeaderHeartbeatTs = 0,
                    followerHeartbeats = mapOf(Pair(NodeID("1"), Time.now()))
            )
            throw AssertionError("Expected to fail if follower has followers")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testCandidateShouldFollowSelf() {
        try {
            State(
                    term = Term(1),
                    vote = NodeID("123"),
                    role = Role.CANDIDATE,
                    leader = null,
                    lastLeaderHeartbeatTs = 0,
                    followerHeartbeats = mapOf()
            )
            throw AssertionError("Expected to fail if candidate does not follow self")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testCandidateShouldVoteSelf() {
        try {
            State(
                    term = Term(1),
                    vote = null,
                    role = Role.CANDIDATE,
                    leader = null,
                    lastLeaderHeartbeatTs = 0,
                    followerHeartbeats = mapOf(Pair(NodeID("123"), Time.now()))
            )
            throw AssertionError("Expected to fail if candidate does not vote for itself")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testLeaderShouldFollowSelf() {
        try {
            State(
                    term = Term(1),
                    vote = NodeID("123"),
                    role = Role.LEADER,
                    leader = NodeID("123"),
                    lastLeaderHeartbeatTs = 0,
                    followerHeartbeats = mapOf()
            )
            throw AssertionError("Expected to fail if leader does not follow self")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testLeaderShouldBeFollowed() {
        try {
            State(
                    term = Term(1),
                    vote = NodeID("123"),
                    role = Role.LEADER,
                    leader = NodeID("123"),
                    lastLeaderHeartbeatTs = 0,
                    followerHeartbeats = mapOf(Pair(NodeID("1"), Time.now()))
            )
            throw AssertionError("Expected to fail if leader does not have at least 2 followers")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testLeaderShouldVoteForSelf() {
        try {
            State(
                    term = Term(1),
                    vote = null,
                    role = Role.CANDIDATE,
                    leader = null,
                    lastLeaderHeartbeatTs = 0,
                    followerHeartbeats = mapOf(
                            Pair(NodeID("1"), Time.now()),
                            Pair(NodeID("2"), Time.now())
                    )
            )
            throw AssertionError("Expected to fail if leader does not vote for itself")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testLeaderHasCorrectTerm() {
        try {
            State(
                    term = Term(0),
                    vote = null,
                    role = Role.LEADER,
                    leader = null,
                    lastLeaderHeartbeatTs = 0,
                    followerHeartbeats = mapOf(Pair(NodeID("123"), Time.now()))
            )
            throw AssertionError("Expected to fail if leader has term equal 0")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testCandidateHasCorrectTerm() {
        try {
            State(
                    term = Term(0),
                    vote = null,
                    role = Role.CANDIDATE,
                    leader = null,
                    lastLeaderHeartbeatTs = 0,
                    followerHeartbeats = mapOf(Pair(NodeID("123"), Time.now()))
            )
            throw AssertionError("Expected to fail if candidate has term equal 0")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testFollowerIsAllowedZeroTerm() {
        State(
                term = Term(num = 0),
                vote = null,
                role = Role.FOLLOWER,
                leader = null,
                lastLeaderHeartbeatTs = 0,
                followerHeartbeats = mapOf()
        )
    }

    private fun testStateCopy() {
        val state = State(
                term = Term(0),
                vote = null,
                role = Role.FOLLOWER,
                leader = NodeID("3"),
                lastLeaderHeartbeatTs = Time.now(),
                followerHeartbeats = mapOf()
        )

        val copyTerm = Term(5)
        val copy = State(state, copyTerm)

        if (copy.term != copyTerm || copy.role != state.role
                || copy.leader != state.leader || copy.lastLeaderHeartbeatTs != state.lastLeaderHeartbeatTs
                || copy.followerHeartbeats != state.followerHeartbeats) {
            throw AssertionError("State 'copy' constructor does not work")
        }
    }
}