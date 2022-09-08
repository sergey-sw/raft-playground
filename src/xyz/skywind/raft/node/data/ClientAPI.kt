package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.model.NodeID
import xyz.skywind.raft.node.model.State.LeaderInfo

interface ClientAPI {

    val nodeID: NodeID

    fun get(key: String): GetOperationResponse

    fun getAll(): GetAllOperationResponse

    fun set(key: String, value: String): SetOperationResponse

    fun remove(key: String): RemoveOperationResponse

    // -------------------------------------------------------- //

    class SetOperationResponse(success: Boolean, leaderInfo: LeaderInfo?): ClientResponse(success, leaderInfo)

    class GetOperationResponse(val data: String?, success: Boolean, leaderInfo: LeaderInfo?): ClientResponse(success, leaderInfo)

    class GetAllOperationResponse(val data: Map<String, String>, success: Boolean, leaderInfo: LeaderInfo?): ClientResponse(success, leaderInfo)

    class RemoveOperationResponse(success: Boolean, leaderInfo: LeaderInfo?): ClientResponse(success, leaderInfo)

    sealed class ClientResponse(val success: Boolean, val leaderInfo: LeaderInfo?)
}