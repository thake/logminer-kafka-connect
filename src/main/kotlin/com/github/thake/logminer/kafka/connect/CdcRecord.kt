package com.github.thake.logminer.kafka.connect

import org.apache.kafka.connect.data.Schema
import java.sql.Timestamp

data class CdcRecord(
    val scn: Long,
    val rowId: String,
    val table: TableId,
    val timestamp: Timestamp,
    val operation: Operation,
    val transaction: String,
    val username: String?,
    val dataSchema: Schema,
    val before: Map<String, Any?>?,
    val after: Map<String, Any?>?
)