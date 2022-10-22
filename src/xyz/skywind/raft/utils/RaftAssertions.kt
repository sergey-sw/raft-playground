package xyz.skywind.raft.utils

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.rpc.VoteRequest
import xyz.skywind.raft.node.model.State
import xyz.skywind.raft.node.model.Term

object RaftAssertions {

    fun verifyRequestHasHigherTerm(state: State, req: VoteRequest) {
        if (req.candidateTerm > state.term)
            return

        throw RaftImplementationException(
            "Assertion failed. " +
                    "We already checked that node did not vote in msg.term = ${req.candidateTerm}. " +
                    "Node is in Candidate or Leader state which means it made a vote for " +
                    "itself (${state.voteInfo?.votedFor}) in state.term = ${state.term}. " +
                    "We can reach this code path only if VoteRequest.term is higher that State.term"
        )
    }

    fun verifyNodeDidNotVoteInThisTerm(state: State, voteTerm: Term, candidate: NodeID) {
        if (voteTerm == state.term) {
            if (state.voteInfo != null) {
                throw RaftImplementationException("Assertion failed. " +
                        "Tried to vote for $candidate, though already voted for ${state.voteInfo.votedFor} " +
                        "in term ${state.term}. Expected to vote only once in a term.")
            }
        }
    }
}