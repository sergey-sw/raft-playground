package xyz.skywind.raft.node.impl

import xyz.skywind.raft.cluster.ClusterConfig
import xyz.skywind.raft.node.debug.LifecycleLogging
import xyz.skywind.raft.node.model.Role
import xyz.skywind.raft.node.model.State
import xyz.skywind.tools.Delay
import xyz.skywind.tools.Logging
import xyz.skywind.tools.Time
import xyz.skywind.tools.Timestamp
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.function.Supplier
import java.util.logging.Level

class TimerTask(private val stateGetter: Supplier<State>,
                private val cfg: ClusterConfig,
                private val logging: LifecycleLogging,
                private val followerTask: Runnable,
                private val candidateTask: Runnable,
                private val leaderTask: Runnable) {

    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    @Volatile
    private var nextExecutionTime: Timestamp = Time.now()

    fun start() {
        resetElectionTimeout()
        executor.scheduleAtFixedRate(Tick(), Time.millis(5000), Time.millis(5000), TimeUnit.NANOSECONDS)
    }

    fun resetElectionTimeout() {
        check(stateGetter.get().role == Role.FOLLOWER) { "Self promotion requires ${Role.FOLLOWER} role" }

        val electionTimeout = getRandomElectionTimeout()
        nextExecutionTime = Time.now() + electionTimeout

        if (stateGetter.get().leaderInfo == null) {
            logging.awaitingSelfPromotion(electionTimeout)
        }
    }

    fun resetHeartbeatTimeout() {
        if (stateGetter.get().role == Role.LEADER) {
            nextExecutionTime = Time.now() + cfg.heartbeatTickPeriod
        }
    }

    private fun execute() {
        check(Time.now() >= nextExecutionTime) { "Attempt to execute task before the specified time" }

        nextExecutionTime = when (stateGetter.get().role) {
            Role.FOLLOWER -> {
                followerTask.run()
                Time.now() + cfg.electionTimeoutMaxMs
            }
            Role.CANDIDATE -> {
                candidateTask.run()
                Time.now() + getRandomElectionTimeout()
            }
            Role.LEADER -> {
                leaderTask.run()
                Time.now() + cfg.heartbeatTickPeriod
            }
        }
    }

    private fun getRandomElectionTimeout(): Long {
        return Delay.between(cfg.electionTimeoutMinMs, cfg.electionTimeoutMaxMs)
    }

    inner class Tick : Runnable {
        override fun run() {
            val nextCheckTimeBefore = nextExecutionTime

            if (Time.now() >= nextCheckTimeBefore) {
                try {
                    execute()
                } catch (e: Throwable) {
                    Logging.getLogger("system").log(Level.SEVERE, e.stackTraceToString())
                    throw e
                }

                check(nextExecutionTime > nextCheckTimeBefore) { "PromotionTask execution must change the timer" }
            }
        }
    }
}