package xyz.skywind.tools

import java.util.*

object Time {

    private const val SPEEDUP_FACTOR = 1.0 // speed up time if you're bored waiting

    fun now(): Timestamp {
        return Timestamp(System.currentTimeMillis())
    }

    fun millis(amount: Int): Long {
        return (amount / SPEEDUP_FACTOR).toLong()
    }

    fun millis(amount: Long): Long {
        return (amount / SPEEDUP_FACTOR).toLong()
    }
}

@JvmInline
value class Timestamp(private val ts: Long): Comparable<Timestamp> {

    operator fun minus(diff: Long): Timestamp {
        return Timestamp(ts - diff)
    }

    operator fun plus(diff: Long): Timestamp {
        return Timestamp(ts + diff)
    }

    operator fun minus(diff: Timestamp): Long {
        return ts - diff.ts
    }

    override fun compareTo(other: Timestamp): Int {
        return ts.compareTo(other.ts)
    }

    fun toDate(): Date {
        return Date(ts)
    }

    override fun toString(): String {
        return ts.toString()
    }
}