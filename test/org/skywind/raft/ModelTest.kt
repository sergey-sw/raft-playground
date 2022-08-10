package org.skywind.raft

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.node.NodeID
import xyz.skywind.raft.node.Role
import xyz.skywind.raft.node.State
import xyz.skywind.raft.node.State.*
import xyz.skywind.raft.node.Term
import xyz.skywind.raft.utils.States
import xyz.skywind.tools.Time

object ModelTest {

    @JvmStatic
    fun main(args: Array<String>) {
        termValidation()
        termComparator()
        testState()

        testSelfPromotion()

        println("Tests passed")
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
        State(
                term = Term(1),
                voteInfo = VoteInfo(NodeID("1"), Time.now()),
                role = Role.CANDIDATE,
                leaderInfo = null,
                followers = makeFollowers("1")
        )

        State(
                term = Term(1),
                voteInfo = VoteInfo(NodeID("1"), Time.now()),
                role = Role.LEADER,
                leaderInfo = LeaderInfo(NodeID("1"), Time.now()),
                followers = makeFollowers("1", "2", "3")
        )

        testOkFollowerStates()

        testFollowerShouldNotHaveFollowers()
        testCandidateShouldFollowSelf()
        testLeaderShouldFollowSelf()
        testLeaderShouldBeFollowed()

        testCandidateShouldVoteSelf()
        testLeaderShouldVoteForSelf()

        testCandidateCantHaveLeader()
        testLeaderHasLeaderProperty()
        testLeaderVotedForSelf()

        testLeaderHasCorrectTerm()
        testCandidateHasCorrectTerm()
        testFollowerIsAllowedZeroTerm()

        testStateCopy()
    }

    private fun testOkFollowerStates() {
        State(
                term = Term(1),
                voteInfo = null,
                role = Role.FOLLOWER,
                leaderInfo = null,
                mapOf()
        )

        State(
                term = Term(1),
                voteInfo = VoteInfo(NodeID("123"), Time.now()),
                role = Role.FOLLOWER,
                leaderInfo = null,
                mapOf()
        )

        State(
                term = Term(1),
                voteInfo = VoteInfo(NodeID("123"), Time.now()),
                role = Role.FOLLOWER,
                leaderInfo = LeaderInfo(NodeID("123"), Time.now()),
                mapOf()
        )
    }

    private fun testFollowerShouldNotHaveFollowers() {
        try {
            State(
                    term = Term(1),
                    voteInfo = null,
                    role = Role.FOLLOWER,
                    leaderInfo = null,
                    followers = makeFollowers("1")
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
                    voteInfo = VoteInfo(NodeID("123"), Time.now()),
                    role = Role.CANDIDATE,
                    leaderInfo = null,
                    followers = mapOf()
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
                    voteInfo = null,
                    role = Role.CANDIDATE,
                    leaderInfo = null,
                    followers = makeFollowers("123")
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
                    voteInfo = VoteInfo(NodeID("123"), Time.now()),
                    role = Role.LEADER,
                    leaderInfo = LeaderInfo(NodeID("123"), Time.now()),
                    followers = mapOf()
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
                    voteInfo = VoteInfo(NodeID("123"), Time.now()),
                    role = Role.LEADER,
                    leaderInfo = LeaderInfo(NodeID("123"), Time.now()),
                    followers = makeFollowers("1")
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
                    voteInfo = null,
                    role = Role.CANDIDATE,
                    leaderInfo = LeaderInfo(NodeID("1"), Time.now()),
                    followers = makeFollowers("1", "2")
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
                    voteInfo = null,
                    role = Role.LEADER,
                    leaderInfo = null,
                    followers = makeFollowers("123")
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
                    voteInfo = null,
                    role = Role.CANDIDATE,
                    leaderInfo = null,
                    followers = makeFollowers("123")
            )
            throw AssertionError("Expected to fail if candidate has term equal 0")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testFollowerIsAllowedZeroTerm() {
        State(
                term = Term(num = 0),
                voteInfo = null,
                role = Role.FOLLOWER,
                leaderInfo = null,
                followers = mapOf()
        )
    }

    private fun testCandidateCantHaveLeader() {
        try {
            State(
                    term = Term(10),
                    voteInfo = VoteInfo(NodeID("candidate"), Time.now()),
                    leaderInfo = LeaderInfo(NodeID("leader"), Time.now()),
                    role = Role.CANDIDATE,
                    followers = makeFollowers("candidate")
            )
            throw AssertionError("Expected to fail if candidate has leader property")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testLeaderHasLeaderProperty() {
        try {
            State(
                    term = Term(10),
                    voteInfo = VoteInfo(NodeID("leader"), Time.now()),
                    leaderInfo = null,
                    role = Role.LEADER,
                    followers = makeFollowers("candidate", "leader")
            )
            throw AssertionError("Expected to fail if leader has unset leader property")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testLeaderVotedForSelf() {
        try {
            State(
                    term = Term(10),
                    voteInfo = VoteInfo(NodeID("c1"), Time.now()),
                    leaderInfo = LeaderInfo(NodeID("leader"), Time.now()),
                    role = Role.LEADER,
                    followers = makeFollowers("c1", "c2")
            )
            throw AssertionError("Expected to fail if leader voted for other node")
        } catch (e: IllegalStateException) {
            return
        }
    }

    private fun testStateCopy() {
        val state = State(
                term = Term(0),
                voteInfo = null,
                role = Role.FOLLOWER,
                leaderInfo = LeaderInfo(NodeID("3"), Time.now()),
                followers = mapOf()
        )

        val copyTerm = Term(5)
        val copy = State(state, copyTerm)

        if (copy.term != copyTerm || copy.role != state.role
                || copy.leaderInfo != state.leaderInfo
                || copy.followers != state.followers) {
            throw AssertionError("State 'copy' constructor does not work")
        }
    }

    private fun testSelfPromotion() {
        val cfg = Config(
                nodeCount = 5,
                electionTimeoutMinMs = 150,
                electionTimeoutMaxMs = 300,
                heartbeatTimeoutMs = 3000
        )

        testInitialStateShouldPromote(cfg)
        testOnlyFollowerShouldPromote(cfg)
        testFollowerShouldPromoteWithoutLeader(cfg)
        testFollowerShouldNotPromoteWithActiveLeader(cfg)
        testFollowerShouldPromoteWithStaleLeader(cfg)
        testFollowerShouldNotPromoteIfVotedRecently(cfg)
    }

    private fun testInitialStateShouldPromote(cfg: Config) {
        check(States.initialState().needSelfPromotion(cfg)) { "Should be able to promo in initial state" }
    }

    private fun testOnlyFollowerShouldPromote(cfg: Config) {
        val candidateState = State(
                term = Term(10),
                voteInfo = VoteInfo(NodeID("candidate"), Time.now()),
                role = Role.CANDIDATE,
                leaderInfo = null,
                followers = makeFollowers("candidate")
        )

        check(!candidateState.needSelfPromotion(cfg)) { "Candidate should not promote" }

        val leaderState = State(
                term = Term(10),
                voteInfo = VoteInfo(NodeID("leader"), Time.now()),
                role = Role.LEADER,
                leaderInfo = LeaderInfo(NodeID("leader"), Time.now()),
                followers = makeFollowers("candidate", "leader")
        )

        check(!leaderState.needSelfPromotion(cfg)) { "Candidate should not promote" }
    }

    private fun testFollowerShouldPromoteWithoutLeader(cfg: Config) {
        val followerState = State(
                term = Term(10),
                voteInfo = null,
                role = Role.FOLLOWER,
                leaderInfo = null,
                followers = mapOf()
        )
        check(followerState.needSelfPromotion(cfg))
    }

    private fun testFollowerShouldNotPromoteWithActiveLeader(cfg: Config) {
        val followerState = State(
                term = Term(10),
                voteInfo = VoteInfo(NodeID("leader"), Time.now()),
                role = Role.FOLLOWER,
                leaderInfo = LeaderInfo(NodeID("leader"), lastHeartbeatTs = Time.now() - (cfg.heartbeatTimeoutMs / 2)),
                followers = mapOf()
        )
        check(!followerState.needSelfPromotion(cfg))
    }

    private fun testFollowerShouldPromoteWithStaleLeader(cfg: Config) {
        val followerState = State(
                term = Term(10),
                voteInfo = VoteInfo(NodeID("leader"), votedAt = Time.now() - 10 * cfg.heartbeatTimeoutMs),
                role = Role.FOLLOWER,
                leaderInfo = LeaderInfo(NodeID("leader"), lastHeartbeatTs = Time.now() - cfg.heartbeatTimeoutMs * 3 / 2),
                followers = mapOf()
        )
        check(followerState.needSelfPromotion(cfg))
    }

    private fun testFollowerShouldNotPromoteIfVotedRecently(cfg: Config) {
        val followerState = State(
                term = Term(10),
                voteInfo = VoteInfo(NodeID("candidate"), votedAt = Time.now() - cfg.electionTimeoutMinMs / 5),
                role = Role.FOLLOWER,
                leaderInfo = null,
                followers = mapOf()
        )
        check(!followerState.needSelfPromotion(cfg))
    }

    // ---------- //
    private fun makeFollowers(vararg nodes: String): Map<NodeID, FollowerInfo> {
        val followers = HashMap<NodeID, FollowerInfo>()
        for (node in nodes) {
            followers[NodeID(node)] = FollowerInfo(Time.now(), 0)
        }
        return followers
    }
}