package com.github.thake.logminer.kafka.connect

import java.sql.Connection

class SelectSource : Source {
    override fun getOffset(): Offset {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun maybeStartQuery(db: Connection) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun poll(): List<PollResult> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun close() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}