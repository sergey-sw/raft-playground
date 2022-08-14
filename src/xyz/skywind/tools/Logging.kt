package xyz.skywind.tools

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger

// prints logs in chronological order
object Logging {

    private val msgQueue = ConcurrentLinkedDeque<LogEntry>()
    private val scheduler = Schedulers.singleThreadDaemonScheduler()
    private val loggerByName = ConcurrentHashMap<String, Logger>()

    init {
        // date - logger name - level - message //
        System.setProperty(
            "java.util.logging.SimpleFormatter.format",
            "%1\$tF %1\$tT.%1\$tL %3\$s %4\$s %5\$s%6\$s%n"
        )

        scheduler.scheduleAtFixedRate(DumpLogs(), 5, 5, TimeUnit.MILLISECONDS)

        Runtime.getRuntime().addShutdownHook(Thread(DumpLogs()))
    }

    private data class LogEntry(val msg: String, val logger: String, val level: Level, val ts: Timestamp = Time.now())

    class MyLogger(private val name: String) {
        fun log(level: Level, msg: String) {
            msgQueue.add(LogEntry(msg, name, level))
        }
    }

    fun getLogger(name: String): MyLogger {
        return MyLogger(name)
    }

    private class DumpLogs: Runnable {
        override fun run() {
            val size = msgQueue.size

            val logs = ArrayList<LogEntry>(size)
            for (i in 0 until size) {
                logs.add(msgQueue.poll())
            }

            logs.sortBy { it.ts }

            for (i in 0 until size) {
                val (msg, logger, level) = logs[i]

                val javaLogger = loggerByName.computeIfAbsent(logger) { Logger.getLogger(it) }
                javaLogger.log(level, msg)
            }
        }
    }
}