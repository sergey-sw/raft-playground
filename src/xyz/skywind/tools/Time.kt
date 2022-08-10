package xyz.skywind.tools

object Time {

    fun now(): Timestamp {
        return Timestamp(System.currentTimeMillis())
    }
}

@JvmInline
value class Timestamp(val ts: Long): Comparable<Timestamp> {
    operator fun minus(diff: Long): Timestamp {
        return Timestamp(ts - diff)
    }

    operator fun plus(diff: Long): Timestamp {
        return Timestamp(ts + diff)
    }

    operator fun minus(diff: Timestamp): Long {
        return ts - diff.ts
    }

    operator fun plus(diff: Timestamp): Long {
        return ts + diff.ts
    }

    override fun compareTo(other: Timestamp): Int {
        return ts.compareTo(other.ts)
    }
}