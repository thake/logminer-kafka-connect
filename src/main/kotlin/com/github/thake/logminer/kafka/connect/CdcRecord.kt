package com.github.thake.logminer.kafka.connect

import java.sql.Timestamp

data class CdcRecord(
    val scn: Long,
    val rowId: String,
    val table: TableId,
    val timestamp: Timestamp,
    val operation: Operation,
    val transaction: String,
    val username: String?,
    val dataSchema: SchemaDefinition,
    val before: Map<String, Any?>?,
    val after: Map<String, Any?>?
)