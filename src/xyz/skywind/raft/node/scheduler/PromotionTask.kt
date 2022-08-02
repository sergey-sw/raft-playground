package xyz.skywind.raft.node.scheduler

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.node.Role
import xyz.skywind.raft.node.log.LifecycleLogging
import xyz.skywind.tools.Delay
import xyz.skywind.tools.Time
import java.util.concurrent.Callable

class PromotionTask(private val roleGetter: Callable<Role>,
                    private val cfg: Config,
                    private val logging: LifecycleLogging,
                    private val scheduler: Scheduler,
                    private val executeWhenFollower: Runnable,
                    private val executeWhenCandidate: Runnable,
                    private val executeWhenLeader: Runnable) {

    @Volatile
    private var nextExecutionTime: Long = Time.now()

    fun start() {
        check(roleGetter.call() == Role.FOLLOWER) { "Expected to start as a ${Role.FOLLOWER}" }

        tryPromoteSelfToCandidateLater()

        scheduler.runPeriodically(5, Tick())
    }

    fun tryPromoteSelfToCandidateLater() {
        check(roleGetter.call() == Role.FOLLOWER) { "Self promotion requires ${Role.FOLLOWER} role" }

        val electionTimeout = getRandomElectionTimeout()
        nextExecutionTime = Time.now() + electionTimeout
        logging.awaitingSelfPromotion(electionTimeout)
    }

    private fun execute() {
        check(Time.now() >= nextExecutionTime) { "Attempt to execute task before the specified time" }

        val role = roleGetter.call()
        checkNotNull(role)

        when (role) {
            Role.FOLLOWER -> {
                executeWhenFollower.run()
                nextExecutionTime = Time.now() + cfg.electionTimeoutMaxMs
            }
            Role.CANDIDATE -> {
                executeWhenCandidate.run()
                nextExecutionTime = Time.now() + getRandomElectionTimeout()
            }
            Role.LEADER -> {
                executeWhenLeader.run()
                nextExecutionTime = Time.now() + cfg.heartbeatTickPeriod
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
                execute()

                check(nextExecutionTime > nextCheckTimeBefore) { "PromotionTask execution must change the timer" }
            }
        }
    }
}