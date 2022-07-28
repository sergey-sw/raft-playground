package xyz.skywind.raft.node.scheduler

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/**
 * All internal Node operations are executed inside Scheduler.
 * That guarantees correct order of operations and thread synchronization
 */
class Scheduler {

    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    fun runNow(runnable: Runnable) {
        executor.execute(runnable)
    }

    fun runLater(delayMillis: Int, runnable: Runnable): ScheduledFuture<*> {
        return executor.schedule(runnable, delayMillis.toLong(), TimeUnit.MILLISECONDS)
    }
}