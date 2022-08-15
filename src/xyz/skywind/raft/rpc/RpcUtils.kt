package xyz.skywind.raft.rpc

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

object RpcUtils {

    fun countSuccess(futures: List<CompletableFuture<HeartbeatResponse?>>): Int {
        return try {
            var count = 0
            for (future in futures) {
                val response = future.get(10, TimeUnit.MILLISECONDS)
                if (response != null) {
                    if (response.ok) {
                        count++
                    }
                }
            }
            count
        } catch (e: TimeoutException) {
            -1
        }
    }
}