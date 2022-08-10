package xyz.skywind.tools

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger

// prints logs in chronological order
object Logging {

    private val msgQueue = ConcurrentLinkedDeque<LogEntry>()
    private val scheduler = Executors.newSingleThreadScheduledExecutor()
    private val loggerByName = ConcurrentHashMap<String, Logger>()

    init {
        scheduler.scheduleAtFixedRate(DumpLogs(), 5, 5, TimeUnit.MILLISECONDS)
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