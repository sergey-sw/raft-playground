package xyz.skywind.raft

import xyz.skywind.raft.cluster.Cluster
import xyz.skywind.raft.node.data.ClientAPI
import xyz.skywind.raft.node.data.ClientAPI.*
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.tools.Logging
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.logging.Level
import kotlin.collections.HashMap

// TODO check log replication conflicts on unexpected leader changes
class DemoClient(val cluster: Cluster, id: Int) : Runnable {

    companion object {
        val OP_LOG = ConcurrentLinkedDeque<Map<String, Any>>()
        val KV: MutableMap<String, String> = Collections.synchronizedMap(HashMap<String, String>())
    }

    private val logger = Logging.getLogger("DemoClient-$id")

    private val ops = listOf("set", "remove")
    private val keys = listOf("A", "B", "C", "D", "E", "F", "G")
    private val values = IntRange(1, 100).map { it.toString() }

    private var lastKnownLeader: NodeID? = null

    override fun run() {
        while (true) {
            Thread.sleep(5_000)
            execute(cluster.getAnyNode(), ops.random(), keys.random(), values.random(), retry = true)
        }
    }

    private fun execute(clientAPI: ClientAPI, op: String, key: String, value: String, retry: Boolean) {
        onBeforeRequest(clientAPI, op, key, value, retry)

        val response = execute(clientAPI, op, key, value)
        if (response.success) {
            lastKnownLeader = clientAPI.nodeID
            onSuccess(op, key, value, clientAPI)
        } else {
            val leader = response.leaderInfo
            if (leader != null) {
                execute(cluster.getNode(leader.leader), op, key, value, retry = false)
            } else {
                lastKnownLeader = null
                onFail(op, key, value, retry, clientAPI)
            }
        }
    }

    private fun onBeforeRequest(clientAPI: ClientAPI, op: String, key: String, value: String, retry: Boolean) {
        var kvLog = "for key=$key"
        if (op == "set") {
            kvLog += ", value=${(value)}"
        }
        val action = if (retry) "Executing" else "Retrying"
        logger.log(Level.INFO, "$action ${op.uppercase()} on node ${clientAPI.nodeID} $kvLog")
    }

    private fun onSuccess(op: String, key: String, value: String, clientAPI: ClientAPI) {
        val entry = entry(status = "ok", op = op, key = key)
        if (op == "set") {
            entry["value"] = value
            KV[key] = value
        } else if (op == "remove") {
            KV.remove(key)
        }

        OP_LOG.add(entry)
        logger.log(Level.INFO, "Executed $op on node ${clientAPI.nodeID}: $entry")
    }

    private fun onFail(op: String, key: String, value: String, retry: Boolean, clientAPI: ClientAPI) {
        val entry = entry(status = "fail", op = op, key = key)
        entry["response"] = if (retry) "[NO LEADER]" else "[UNKNOWN]"
        if (op == "set") {
            entry["value"] = value
        }
        OP_LOG.add(entry)
        logger.log(Level.INFO, "Failed on node ${clientAPI.nodeID}: $entry")
    }

    private fun entry(status: String, op: String, key: String): HashMap<String, String> {
        return HashMap(
            mapOf(
                Pair("status", status),
                Pair("op", op),
                Pair("key", key)
            )
        )
    }

    private fun execute(node: ClientAPI, op: String, key: String, value: String): ClientResponse {
        return when (op) {
            "get" -> node.get(key)
            "set" -> node.set(key, value)
            "remove" -> node.remove(key)
            else -> throw IllegalArgumentException(op)
        }
    }
}