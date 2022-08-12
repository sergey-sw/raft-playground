package xyz.skywind.raft.rpc

import java.util.concurrent.CompletableFuture

object RpcUtils {

    fun countSuccess(futures: List<CompletableFuture<HeartbeatResponse?>>): Int {
        var count = 0
        for (future in futures) {
            val response = future.join()
            if (response != null) {
                if (response.ok) {
                    count++
                }
            }
        }
        return count
    }
}