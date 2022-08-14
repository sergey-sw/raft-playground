package xyz.skywind.raft

import xyz.skywind.raft.cluster.Cluster
import xyz.skywind.raft.node.data.ClientAPI
import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.tools.Logging
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.logging.Level
import kotlin.collections.HashMap

// TODO have two problems:
// problem 1: data operation is @synchronized. Leader waits for response and holds lock, response never executes
// problem 2: followers don't want to apply op: """raft-node-n1 INFO No ops were applied. Leader commit idx: -1, follower commit idx: -1, apply idx: -1"""
class DemoClient(val cluster: Cluster, id: Int) : Runnable {

    companion object {
        val OP_LOG = ConcurrentLinkedDeque<Map<String, Any>>()
        val KV: MutableMap<String, ByteArray> = Collections.synchronizedMap(HashMap<String, ByteArray>())
    }

    private val logger = Logging.getLogger("DemoClient-$id")

    private val ops = listOf("get", "set", "remove")
    private val keys = listOf("A", "B", "C", "D", "E", "F", "G")
    private val values = IntRange(1, 100).map { it.toString().toByteArray() }

    private var lastKnownLeader: NodeID? = null

    override fun run() {
        while (true) {
            Thread.sleep(5_000)

            val node = cluster.getAnyNode()

            when (ops.random()) {
                "get" -> executeGet(node, withRetry = true)
                "set" -> executeSet(node, withRetry = true)
                "remove" -> executeRemove(node, withRetry = true)
            }
        }
    }

    private fun executeGet(node: ClientAPI, withRetry: Boolean) {
        val key = keys.random()

        val action = if (withRetry) "Executing" else "Retrying"
        logger.log(Level.INFO, "$action GET on node ${node.nodeID} for key=$key")

        val response = node.get(key)
        if (response.success) {
            val data = response.data

            val logEntry = mapOf(
                Pair("status", "ok"),
                Pair("op", "get"),
                Pair("key", key),
                Pair("response", if (data != null) String(data) else "-")
            )

            OP_LOG.add(logEntry)
            logger.log(Level.INFO, "Executed GET on node ${node.nodeID}: $logEntry")

            lastKnownLeader = node.nodeID
        } else if (withRetry) {
            val leaderInfo = response.leaderInfo
            if (leaderInfo != null) {
                lastKnownLeader = response.leaderInfo.leader
                executeGet(cluster.getNode(leaderInfo.leader), withRetry = false)
            } else {
                OP_LOG.add(
                    mapOf(
                        Pair("status", "fail"),
                        Pair("op", "get"),
                        Pair("key", key),
                        Pair("response", "[NO LEADER]")
                    )
                )
                lastKnownLeader = null
                logger.log(Level.INFO, "Failed GET key=$key on node ${node.nodeID}: no leader")
            }
        }
    }

    private fun executeSet(node: ClientAPI, withRetry: Boolean) {
        val key = keys.random()
        val value = values.random()

        val action = if (withRetry) "Executing" else "Retrying"
        logger.log(Level.INFO, "$action SET on node ${node.nodeID} for key=$key, value=${String(value)}")

        val response = node.set(key, value)
        if (response.success) {
            val logEntry = mapOf(
                Pair("status", "ok"),
                Pair("op", "set"),
                Pair("key", key),
                Pair("value", String(value)),
            )
            OP_LOG.add(logEntry)

            KV[key] = value
            logger.log(Level.INFO, "Executed SET on node ${node.nodeID}: $logEntry")

            lastKnownLeader = node.nodeID
        } else if (withRetry) {
            val leaderInfo = response.leaderInfo
            if (leaderInfo != null) {
                lastKnownLeader = node.nodeID
                executeSet(cluster.getNode(leaderInfo.leader), withRetry = false)
            } else {
                OP_LOG.add(
                    mapOf(
                        Pair("status", "fail"),
                        Pair("op", "set"),
                        Pair("key", key),
                        Pair("value", String(value)),
                        Pair("response", "[NO LEADER]")
                    )
                )
                lastKnownLeader = null
                logger.log(Level.INFO, "Failed SET key=$key, value=${String(value)} on node ${node.nodeID}: no leader")
            }
        }
    }

    private fun executeRemove(node: ClientAPI, withRetry: Boolean) {
        val key = keys.random()

        val action = if (withRetry) "Executing" else "Retrying"
        logger.log(Level.INFO, "$action REMOVE on node ${node.nodeID} for key=$key")

        val response = node.remove(key)
        if (response.success) {
            OP_LOG.add(
                mapOf(
                    Pair("status", "ok"),
                    Pair("op", "set"),
                    Pair("key", key),
                )
            )
            KV.remove(key)

            lastKnownLeader = node.nodeID
        } else if (withRetry) {
            val leaderInfo = response.leaderInfo
            if (leaderInfo != null) {
                lastKnownLeader = node.nodeID
                executeRemove(cluster.getNode(leaderInfo.leader), withRetry = false)
            } else {
                OP_LOG.add(
                    mapOf(
                        Pair("status", "fail"),
                        Pair("op", "remove"),
                        Pair("key", key),
                        Pair("response", "[NO LEADER]")
                    )
                )
                lastKnownLeader = null
                logger.log(Level.INFO, "Failed REMOVE key=$key on node ${node.nodeID}: no leader")
            }
        }
    }
}