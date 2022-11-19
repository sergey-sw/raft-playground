package xyz.skywind.tools

import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.collections.ArrayList

// prints logs in chronological order
object Logging {

    private var ENABLE_FILE_LOGGING = true

    private val msgQueue = ConcurrentLinkedDeque<LogEntry>()
    private val scheduler = Schedulers.singleThreadDaemonScheduler()
    private val loggerByName = ConcurrentHashMap<String, Logger>()

    private val LOGGING_FILE_NAME = "/tmp/raft-log-" + SimpleDateFormat("yyyy-MM-dd----HH-mm").format(Date())
    private val TIME_PATTERN = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    init {
        // date - logger name - level - message //
        System.setProperty(
            "java.util.logging.SimpleFormatter.format",
            "%1\$tF %1\$tT.%1\$tL %3\$s %4\$s %5\$s%6\$s%n"
        )

        if (ENABLE_FILE_LOGGING) {
            try {
                Files.createFile(Paths.get(LOGGING_FILE_NAME))
                msgQueue.add(LogEntry("Logging to $LOGGING_FILE_NAME", "raft-cluster", Level.INFO))
            } catch (e: Exception) {
                val msg = "Failed to setup file logging:\n" + e.stackTraceToString()
                msgQueue.add(LogEntry(msg, "raft-cluster", Level.INFO))
                ENABLE_FILE_LOGGING = false
            }
        }

        scheduler.scheduleAtFixedRate(DumpLogs(), 5, 5, TimeUnit.MILLISECONDS)

        Runtime.getRuntime().addShutdownHook(Thread(DumpLogs()))
    }

    private data class LogEntry(val msg: String, val logger: String, val level: Level, val ts: Timestamp = Time.now())

    class MyLogger(private val name: String) {
        fun log(level: Level, msg: String) {
            msgQueue.add(LogEntry(msg, name, level))
        }

        fun warn(msg: String) {
            msgQueue.add(LogEntry(msg, name, Level.WARNING))
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

            val sb = StringBuilder()

            for (i in 0 until size) {
                val (msg, logger, level) = logs[i]

                val javaLogger = loggerByName.computeIfAbsent(logger) { Logger.getLogger(it) }
                javaLogger.log(level, msg)

                sb.append(format(logs[i])).append("\n")
            }

            if (ENABLE_FILE_LOGGING) {
                Files.write(Paths.get(LOGGING_FILE_NAME), sb.toString().toByteArray(), StandardOpenOption.APPEND)
            }
        }

        private fun format(e: LogEntry): String {
            return TIME_PATTERN.format(e.ts.toDate()) + " " + e.logger + " " + e.level + " " + e.msg
        }
    }
}