package xyz.skywind.raft.node

import xyz.skywind.raft.msg.VoteRequest

object RaftAssertions {

    fun verifyRequestHasHigherTerm(state: State, msg: VoteRequest) {
        check(msg.term > state.term) {
            "Assertion failed. " +
                    "We already checked that node did not vote in msg.term = ${msg.term}. " +
                    "Node is in Candidate or Leader state which means it made a vote for " +
                    "itself (${state.vote}) in state.term = ${state.term}. " +
                    "We can reach this code path only if VoteRequest.term is higher that State.term"
        }
    }
}