package xyz.skywind.raft.rpc

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.State
import xyz.skywind.raft.node.model.Term
import xyz.skywind.raft.node.data.LogEntryInfo
import xyz.skywind.raft.node.data.op.Operation

data class AppendEntries(
    val term: Term, // leader's term
    val leader: NodeID,
    val prevLogEntryInfo: LogEntryInfo, // index and term of entry preceding new entries
    val entries: List<Operation> = listOf(),
    val commitIndex: Int // leader's commit index
) {
    companion object {
        operator fun invoke(state: State, prevLogEntryInfo: LogEntryInfo, entries: List<Operation>): AppendEntries {
            checkNotNull(state.leaderInfo)

            return AppendEntries(
                    term = state.term,
                    leader = state.leaderInfo.leader,
                    prevLogEntryInfo = prevLogEntryInfo,
                    entries = entries,
                    commitIndex = state.commitIdx
            )
        }
    }

    override fun toString(): String {
        return "AppendEntries(prev=$prevLogEntryInfo, commitIndex=$commitIndex, entries.size=${entries.size})"
    }
}

