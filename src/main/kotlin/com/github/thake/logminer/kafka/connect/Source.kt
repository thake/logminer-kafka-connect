package com.github.thake.logminer.kafka.connect

import java.sql.Connection

interface Source<T : Offset?> : AutoCloseable {
    fun getOffset(): T
    fun maybeStartQuery(db: Connection)
    fun poll(): List<PollResult>
}