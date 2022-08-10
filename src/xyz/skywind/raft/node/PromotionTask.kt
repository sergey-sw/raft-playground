package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.tools.Delay
import xyz.skywind.tools.Logging
import xyz.skywind.tools.Time
import xyz.skywind.tools.Timestamp
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.function.Supplier
import java.util.logging.Level

class PromotionTask(private val stateGetter: Supplier<State>,
                    private val cfg: Config,
                    private val logging: LifecycleLogging,
                    private val executeWhenFollower: Runnable,
                    private val executeWhenCandidate: Runnable,
                    private val executeWhenLeader: Runnable) {

    private val executor: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    @Volatile
    private var nextExecutionTime: Timestamp = Time.now()

    fun start() {
        check(stateGetter.get().role == Role.FOLLOWER) { "Expected to start as a ${Role.FOLLOWER}" }

        resetElectionTimeout()

        executor.scheduleAtFixedRate(Tick(), 5, 5, TimeUnit.MILLISECONDS)
    }

    fun resetElectionTimeout() {
        check(stateGetter.get().role == Role.FOLLOWER) { "Self promotion requires ${Role.FOLLOWER} role" }

        val electionTimeout = getRandomElectionTimeout()
        nextExecutionTime = Time.now() + electionTimeout

        if (stateGetter.get().leaderInfo == null) {
            logging.awaitingSelfPromotion(electionTimeout)
        }
    }

    private fun execute() {
        check(Time.now() >= nextExecutionTime) { "Attempt to execute task before the specified time" }

        nextExecutionTime = when (stateGetter.get().role) {
            Role.FOLLOWER -> {
                executeWhenFollower.run()
                Time.now() + cfg.electionTimeoutMaxMs
            }
            Role.CANDIDATE -> {
                executeWhenCandidate.run()
                Time.now() + getRandomElectionTimeout()
            }
            Role.LEADER -> {
                executeWhenLeader.run()
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