package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.data.op.Operation

class Data {

    companion object {
        val EMPTY_VALUE = ByteArray(0)
    }

    private val log = OperationLog()

    private val kv = HashMap<String, ByteArray>()

    fun getByKey(key: String): ByteArray {
        return kv[key] ?: EMPTY_VALUE
    }

    fun append(op: Operation): LogEntryInfo {
        return log.append(op)
    }

    fun getLastEntry(): LogEntryInfo {
        return log.getLastEntry()
    }
}