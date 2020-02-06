package com.github.thake.logminer.kafka.connect

import java.sql.Connection

interface Source : AutoCloseable {
    fun getOffset(): Offset
    fun maybeStartQuery(db: Connection)
    fun poll(): List<PollResult>
}