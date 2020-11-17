package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.SourceRecordFields.sourceSchema
import mu.KotlinLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.source.SourceRecord

private val logger = KotlinLogging.logger {}

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
    private const val OWNER = "schema"
    private const val TABLE = "table"
    private const val CHANGE_USER = "user"
    val sourceSchema: Schema = SchemaBuilder.struct().name(LogminerSourceConnector::class.java.`package`.name + ".Source")
            .field(VERSION, Schema.STRING_SCHEMA)
            .field(CONNECTOR, Schema.STRING_SCHEMA)
            .field(RECORD_TIMESTAMP, Timestamp.SCHEMA)
            .field(TRANSACTION, Schema.OPTIONAL_STRING_SCHEMA)
            .field(SCN, Schema.INT64_SCHEMA)
            .field(OWNER, Schema.STRING_SCHEMA)
            .field(TABLE, Schema.STRING_SCHEMA)
            .field(CHANGE_USER, Schema.OPTIONAL_STRING_SCHEMA)
            .build()

    fun convert(cdcRecord: CdcRecord): Struct {
        return Struct(sourceSchema)
                .put(VERSION, LogminerSourceConnector.version)
                .put(CONNECTOR, LogminerSourceConnector.name)
                .put(RECORD_TIMESTAMP, cdcRecord.timestamp)
                .put(TRANSACTION, cdcRecord.transaction)
                .put(SCN, cdcRecord.scn)
                .put(OWNER, cdcRecord.table.owner)
                .put(TABLE, cdcRecord.table.table)
                .put(CHANGE_USER, cdcRecord.username)
    }
}

class ConnectSchemaFactory(
    private val nameService: ConnectNameService,
    private val isEmittingTombstones : Boolean
) {


    private fun createKeyStruct(cdcRecord: CdcRecord): Struct {
        val schema = cdcRecord.dataSchema.keySchema
        val struct = Struct(schema)
        val sourceMap = when (cdcRecord.operation) {
            Operation.READ, Operation.INSERT ->
                cdcRecord.after
            Operation.DELETE, Operation.UPDATE -> cdcRecord.before
        }!!
        schema.fields().forEach {
            struct.put(it.name(), sourceMap[it.name()])
        }
        return struct
    }

    private fun createValue(record: CdcRecord): Pair<Schema, Struct> {
        val name = nameService.getValueRecordName(record.table)
        val recordConnectSchema = record.dataSchema.valueSchema

        val valueSchema = SchemaBuilder.struct()
                .name(name)
                .field(CdcRecordFields.OPERATION, Schema.STRING_SCHEMA)
                .field(CdcRecordFields.BEFORE, recordConnectSchema)
                .field(CdcRecordFields.AFTER, recordConnectSchema)
                .field(CdcRecordFields.SOURCE, sourceSchema)
                .field(CdcRecordFields.PUBLISH_TIMESTAMP, Timestamp.SCHEMA)
                .optional()
                .build()
        val struct = with(record) {
            var updatedAfter = after

            val sourceStruct = SourceRecordFields.convert(record)
            val recordStruct = Struct(valueSchema)
                    .put(CdcRecordFields.OPERATION, operation.stringRep)
                    .put(CdcRecordFields.SOURCE, sourceStruct)
                    .put(CdcRecordFields.PUBLISH_TIMESTAMP, java.util.Date())
            if (operation == Operation.UPDATE && updatedAfter != null && before != null) {
                //Enrich the after state with values from the before data set
                val enrichedAfter = updatedAfter.toMutableMap()
                before.forEach { enrichedAfter.putIfAbsent(it.key, it.value) }
                updatedAfter = enrichedAfter
            }
            before?.let {
                recordStruct.put(
                    CdcRecordFields.BEFORE,
                    convertDataToStruct(recordConnectSchema, it)
                )
            }
            updatedAfter?.let {
                recordStruct.put(
                    CdcRecordFields.AFTER,
                    convertDataToStruct(recordConnectSchema, it)
                )
            }
            recordStruct
        }
        return Pair(valueSchema, struct)
    }

    fun convertToSourceRecords(pollResult: PollResult, partition: Map<String, Any?>): List<SourceRecord> {

        val record = pollResult.cdcRecord
        val topic = nameService.getTopicName(record.table)

        val value = createValue(record)
        val keyStruct = createKeyStruct(record)
        val normalSourceRecord =  SourceRecord(
            partition,
            pollResult.offset.map,
            topic,
            record.dataSchema.keySchema,
            keyStruct,
            value.first,
            value.second
        )
        return if(isEmittingTombstones && pollResult.cdcRecord.operation == Operation.DELETE){
            val deleteRecord = SourceRecord(
                partition,
                pollResult.offset.map,
                topic,
                record.dataSchema.keySchema,
                keyStruct,
                null,
                null
            )
            listOf(
                normalSourceRecord,
                deleteRecord
            )
        }else{
            listOf(normalSourceRecord)
        }

    }

    private fun convertDataToStruct(dataSchema: Schema, values: Map<String, Any?>): Struct {
        return Struct(dataSchema).apply {
            values.forEach { this.put(it.key, it.value) }
        }
    }
}