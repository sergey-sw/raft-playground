package xyz.skywind.tools

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

object Schedulers {

    fun singleThreadDaemonScheduler(): ScheduledExecutorService {
        return Executors.newSingleThreadScheduledExecutor {
            val t = Executors.defaultThreadFactory().newThread(it)
            t.isDaemon = true
            return@newSingleThreadScheduledExecutor t
        }
    }
}