package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.SourceRecordFields.sourceSchema
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.source.SourceRecord

object CdcRecordFields {

    const val OPERATION = "op"
    const val SOURCE = "source"
    const val PUBLISH_TIMESTAMP = "ts_ms"
    const val BEFORE = "before"
    const val AFTER = "after"
}

object SourceRecordFields {
    private const val VERSION = "version"
    private const val CONNECTOR = "connector"
    private const val RECORD_TIMESTAMP = "ts_ms"
    private const val TRANSACTION = "txId"
    private const val SCN = "scn"
    const val NAME = "name"
    private const val OWNER = "schema"
    private const val TABLE = "table"
    const val CHANGE_USER = "user"
    val sourceSchema: Schema = SchemaBuilder.struct().name("source")
            .field(VERSION, Schema.STRING_SCHEMA)
            .field(CONNECTOR, Schema.STRING_SCHEMA)
            .field(RECORD_TIMESTAMP, Timestamp.SCHEMA)
            .field(TRANSACTION, Schema.OPTIONAL_STRING_SCHEMA)
            .field(SCN, Schema.INT64_SCHEMA)
            .field(OWNER, Schema.STRING_SCHEMA)
            .field(TABLE, Schema.STRING_SCHEMA)
            .field(CHANGE_USER, Schema.OPTIONAL_STRING_SCHEMA)
            .build()

    fun convert(pollResult: PollResult): Struct {
        return Struct(sourceSchema).put(VERSION, LogminerSourceConnector.version)
                .put(CONNECTOR, LogminerSourceConnector.name).put(RECORD_TIMESTAMP, pollResult.cdcRecord.timestamp)
                .put(TRANSACTION, pollResult.cdcRecord.transaction).put(SCN, pollResult.cdcRecord.scn)
                .put(OWNER, pollResult.cdcRecord.table.owner).put(TABLE, pollResult.cdcRecord.table.table)
                .put(CHANGE_USER, pollResult.cdcRecord.username)
    }
}

class ConnectSchemaFactory(private val recordPrefix: String) {


    fun convertToSourceRecord(pollResult: PollResult, partition: Map<String, Any?>, topic: String): SourceRecord {
        val record = pollResult.cdcRecord
        val name = recordPrefix + record.table.table + "ChangeRecord"

        val newSchema = SchemaBuilder.struct()
                .name(name)
                .field(CdcRecordFields.OPERATION, Schema.STRING_SCHEMA)
                .field(CdcRecordFields.BEFORE, record.dataSchema)
                .field(CdcRecordFields.AFTER, record.dataSchema)
                .field(CdcRecordFields.SOURCE, sourceSchema)
                .field(CdcRecordFields.PUBLISH_TIMESTAMP, Timestamp.SCHEMA)
                .build()
        val struct = with(record) {
            var updatedAfter = after

            val sourceStruct = SourceRecordFields.convert(pollResult)
            val recordStruct = Struct(newSchema).put(CdcRecordFields.OPERATION, operation.stringRep)
                    .put(CdcRecordFields.SOURCE, sourceStruct)
                    .put(CdcRecordFields.PUBLISH_TIMESTAMP, java.util.Date())
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