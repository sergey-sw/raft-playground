package xyz.skywind.tools

import java.util.*

object Delay {

    private val rnd = Random()

    fun between(from: Int, to: Int): Int {
        return rnd.nextInt(from, to)
    }

    fun between(from: Long, to: Long): Long {
        return rnd.nextLong(from, to)
    }

    fun upTo(value: Int): Int {
        return rnd.nextInt(1, value)
    }
}