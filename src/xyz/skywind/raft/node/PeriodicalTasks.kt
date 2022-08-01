package xyz.skywind.raft.node

import xyz.skywind.raft.cluster.Config
import xyz.skywind.raft.node.log.LifecycleLogging
import xyz.skywind.tools.Delay
import xyz.skywind.tools.Time
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import kotlin.math.max

class PeriodicalTasks(private val node: NodeImpl,
                      private val cfg: Config,
                      private val logging: LifecycleLogging) {

    private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

    @Volatile
    private var nextSelfPromotionTs: Long? = Time.now()
    @Volatile
    private var nextFollowersSyncTs: Long? = Time.now()

    fun start() {
        val electionTimeout = getRandomElectionTimeout()
        nextSelfPromotionTs = Time.now() + electionTimeout
        logging.awaitingSelfPromotion(electionTimeout)

        val tickPeriod = max(1L, cfg.heartbeatTimeoutMs / 10L)
        scheduler.scheduleAtFixedRate(Tick(), tickPeriod, tickPeriod, TimeUnit.MILLISECONDS)
    }

    fun restart() {
        val electionTimeout = getRandomElectionTimeout()
        nextSelfPromotionTs = Time.now() + electionTimeout
    }

    private fun getRandomElectionTimeout(): Int {
        return Delay.between(cfg.electionTimeoutMinMs, cfg.electionTimeoutMaxMs)
    }

    inner class Tick : Runnable {
        override fun run() {
            val promoTs = nextSelfPromotionTs
            if (promoTs != null && promoTs >= Time.now()) {
                node.maybePromoteMeAsLeader()
            }
        }
    }
}