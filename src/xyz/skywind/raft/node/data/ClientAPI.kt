package xyz.skywind.raft.node.data

import xyz.skywind.raft.node.model.State.LeaderInfo

interface ClientAPI {

    fun get(key: String): GetOperationResponse

    fun set(key: String, value: ByteArray): SetOperationResponse

    fun remove(key: String): RemoveOperationResponse

    // -------------------------------------------------------- //

    class SetOperationResponse(val success: Boolean, val leaderInfo: LeaderInfo?)

    class GetOperationResponse(val success: Boolean, val data: ByteArray?, val leaderInfo: LeaderInfo?)

    class RemoveOperationResponse(val success: Boolean, val leaderInfo: LeaderInfo?)
}