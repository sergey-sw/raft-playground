package xyz.skywind.demo

import xyz.skywind.raft.cluster.Cluster
import xyz.skywind.raft.node.data.ClientAPI
import xyz.skywind.raft.node.data.ClientAPI.*
import xyz.skywind.raft.node.impl.DataNode
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.tools.Delay
import xyz.skywind.tools.Logging
import xyz.skywind.tools.Time
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.logging.Level
import kotlin.collections.HashMap
import kotlin.system.exitProcess

class LogReplicationDemoClient(val cluster: Cluster<DataNode>, id: Int) : Runnable {

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
            Thread.sleep(Time.millis(Delay.between(1000L, 2000L)))

            execute(
                clientAPI = getBackendNode(),
                op = ops.random(),
                key = keys.random(),
                value = values.random(),
                canRetry = true
            )

            checkDataConsistency()
        }
    }

    private fun getBackendNode(): ClientAPI {
        return if (lastKnownLeader != null)
            cluster.getNode(lastKnownLeader!!)
        else
            cluster.getAnyNode()
    }

    private fun execute(clientAPI: ClientAPI, op: String, key: String, value: String, canRetry: Boolean) {
        onBeforeRequest(clientAPI, op, key, value, canRetry)

        val response = execute(clientAPI, op, key, value)
        if (response.success) {
            lastKnownLeader = clientAPI.nodeID
            onSuccess(op, key, value, clientAPI)
        } else {
            val leader = response.leaderInfo
            if (leader != null && clientAPI.nodeID != leader.leader) {
                execute(cluster.getNode(leader.leader), op, key, value, canRetry = false)
            } else {
                var reason = "UNKNOWN"
                if (leader == null) {
                    lastKnownLeader = null
                    reason = "NO LEADER"
                }
                onFail(op, key, value, reason, clientAPI)
            }
        }
    }

    private fun onBeforeRequest(clientAPI: ClientAPI, op: String, key: String, value: String, canRetry: Boolean) {
        var kvLog = "for key=$key"
        if (op == "set") {
            kvLog += ", value=${(value)}"
        }
        val action = if (canRetry) "Executing" else "Retrying"
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
        logger.log(Level.INFO, "Executed '$op' on node ${clientAPI.nodeID}: $entry")
    }

    private fun onFail(op: String, key: String, value: String, reason: String, clientAPI: ClientAPI) {
        val entry = entry(status = "fail", op = op, key = key)
        entry["response"] = reason
        if (op == "set") {
            entry["value"] = value
        }
        OP_LOG.add(entry)
        logger.log(Level.INFO, "Failed '$op' on node ${clientAPI.nodeID}: $entry")
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

    private fun checkDataConsistency() {
        val leader = lastKnownLeader
        if (leader != null) {
            val response = cluster.getNode(leader).getAll()
            if (response.success) {
                val leaderData = response.data

                if (leaderData != KV) {
                    val msg = "Leader state is inconsistent with global state.\n" +
                            "Leader: $leaderData \n" + "Global: $KV"
                    logger.log(Level.SEVERE, msg)
                    fail()
                }
            }
        }
    }

    private fun fail() {
        Thread.sleep(Time.millis(5_000))
        exitProcess(0)
    }
}