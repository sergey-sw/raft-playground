package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.data.op.Operation
import xyz.skywind.raft.node.data.op.RemoveValueOperation
import xyz.skywind.raft.node.data.op.SetValueOperation
import xyz.skywind.raft.rpc.AppendEntries
import xyz.skywind.tools.Logging
import java.util.logging.Level

class Data(nodeID: NodeID) {

    companion object {
        val EMPTY_VALUE = ByteArray(0)
    }

    private val logger = Logging.getLogger("Data-$nodeID")

    private val log = OperationLog(nodeID)

    private val kv = HashMap<String, ByteArray>()

    fun getByKey(key: String): ByteArray {
        return kv[key] ?: EMPTY_VALUE
    }

    @Synchronized
    fun applyOperation(op: SetValueOperation) {
        kv[op.key] = op.value
    }

    @Synchronized
    fun applyOperation(op: RemoveValueOperation) {
        kv.remove(op.key)
    }

    @Synchronized
    fun appendOnLeader(op: Operation): LogEntryInfo {
        return log.append(op)
    }

    @Synchronized
    fun removeLastOperation() {
        return log.removeLast()
    }

    @Synchronized
    fun appendOnFollower(prevLogEntryInfo: LogEntryInfo, entries: List<Operation>) {
        log.append(prevLogEntryInfo, entries)
    }

    @Synchronized
    fun getLastEntry(): LogEntryInfo {
        return log.getLastEntry()
    }

    @Synchronized
    fun matchesLeaderLog(req: AppendEntries): Boolean {
        val matches = log.contains(req.prevLogEntryInfo)

        if (!matches) {
            logger.log(Level.WARNING, "Node last entry ${getLastEntry()} does not match with " +
                    "leader's prev entry ${req.prevLogEntryInfo}")
        }

        return matches
    }
}