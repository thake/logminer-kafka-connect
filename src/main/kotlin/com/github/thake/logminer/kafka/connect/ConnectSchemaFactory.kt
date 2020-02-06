package com.github.thake.logminer.kafka.connect

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.source.SourceRecord

object CdcRecordFields {
    const val SCN = "scn"
    const val OWNER = "owner"
    const val TABLE = "table"
    const val TIMESTAMP = "timestamp"
    const val OPERATION = "operation"
    const val BEFORE = "before"
    const val AFTER = "after"
}

class ConnectSchemaFactory(private val recordPrefix: String) {
    fun convertToSourceRecord(pollResult: PollResult, partition: Map<String, Any?>, topic: String): SourceRecord {
        val record = pollResult.cdcRecord
        val name = recordPrefix + record.table.table + "ChangeRecord"
        val newSchema = SchemaBuilder.struct()
                .name(name)
                .field(CdcRecordFields.SCN, Schema.INT64_SCHEMA)
                .field(CdcRecordFields.OWNER, Schema.STRING_SCHEMA)
                .field(CdcRecordFields.TABLE, Schema.STRING_SCHEMA)
                .field(CdcRecordFields.TIMESTAMP, Timestamp.SCHEMA)
                .field(CdcRecordFields.OPERATION, Schema.STRING_SCHEMA)
                .field(CdcRecordFields.BEFORE, record.dataSchema)
                .field(CdcRecordFields.AFTER, record.dataSchema)
                .build()
        val struct = with(record) {
            var updatedAfter = after
            val recordStruct =
                Struct(newSchema).put(CdcRecordFields.SCN, scn).put(CdcRecordFields.OWNER, table.owner)
                        .put(CdcRecordFields.TABLE, table.table).put(CdcRecordFields.TIMESTAMP, timestamp)
                        .put(CdcRecordFields.OPERATION, operation.toString())
            if (operation == Operation.UPDATE && updatedAfter != null && before != null) {
                //Enrich the after state with values from the before data set
                val enrichedAfter = updatedAfter.toMutableMap()
                before.forEach { enrichedAfter.putIfAbsent(it.key, it.value) }
                updatedAfter = enrichedAfter
            }
            before?.let {
                recordStruct.put(CdcRecordFields.BEFORE, convertDataToStruct(dataSchema, it))
            }
            updatedAfter?.let {
                recordStruct.put(CdcRecordFields.AFTER, convertDataToStruct(dataSchema, it))
            }
            recordStruct
        }
        return SourceRecord(partition, pollResult.offset.map, topic, newSchema, struct)
    }

    private fun convertDataToStruct(dataSchema: Schema, values: Map<String, Any?>): Struct {
        return Struct(dataSchema).apply {
            values.forEach { this.put(it.key, it.value) }
        }
    }
}