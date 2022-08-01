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
                    private val executeWhenCandidate: Runnable) {

    @Volatile
    private var nextCheckTime: Long = Time.now()

    fun start() {
        check(roleGetter.call() == Role.FOLLOWER) { "Expected to start as a ${Role.FOLLOWER}" }

        val electionTimeout = getRandomElectionTimeout()
        logging.awaitingSelfPromotion(electionTimeout)
        nextCheckTime = Time.now() + electionTimeout

        scheduler.runPeriodically(10, Tick())
    }

    fun restart(needFullTimeout: Boolean) {
        val electionTimeout = if (needFullTimeout) cfg.electionTimeoutMaxMs else getRandomElectionTimeout()
        nextCheckTime = Time.now() + electionTimeout
        logging.awaitingSelfPromotion(electionTimeout)
    }

    private fun execute() {
        check(Time.now() >= nextCheckTime) { "Attempt to execute task before the specified time" }

        val role = roleGetter.call()
        checkNotNull(role)

        when (role) {
            Role.FOLLOWER -> {
                executeWhenFollower.run()
                restart(needFullTimeout = true)
            }
            Role.CANDIDATE -> {
                executeWhenCandidate.run()
                restart(needFullTimeout = false)
            }
            Role.LEADER -> {}
        }
    }

    private fun getRandomElectionTimeout(): Long {
        return Delay.between(cfg.electionTimeoutMinMs, cfg.electionTimeoutMaxMs)
    }

    inner class Tick : Runnable {
        override fun run() {
            val nextCheckTimeBefore = nextCheckTime

            if (Time.now() >= nextCheckTimeBefore) {
                execute()

                check(nextCheckTime > nextCheckTimeBefore) { "PromotionTask execution must change the timer" }
            }
        }
    }
}