package com.github.thake.logminer.kafka.connect.initial

import com.github.thake.logminer.kafka.connect.*
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

data class FetcherOffset(
    val table: TableId,
    val asOfScn: Long,
    val rowId: String?
)

class TableFetcher(val conn: Connection, val fetcherOffset: FetcherOffset, val schemaService: SchemaService) {
    private val stmt: PreparedStatement
    private val resultSet: ResultSet
    private val schemaDefinition: SchemaDefinition

    init {

        fun determineQuery(): String {
            val rowIdCondition = fetcherOffset.rowId?.let { "WHERE ROWID > '$it'" } ?: ""
            return "SELECT t.*, ROWID, ORA_ROWSCN, SCN_TO_TIMESTAMP(ORA_ROWSCN) as SCN_TIMESTAMP FROM ${fetcherOffset.table.fullName} AS OF SCN ${fetcherOffset.asOfScn} t $rowIdCondition order by ROWID ASC"
        }
        schemaDefinition = schemaService.getSchema(conn, fetcherOffset.table)
        stmt = conn.prepareStatement(determineQuery())
        resultSet = stmt.executeQuery()
    }

    fun poll(): PollResult? {
        return if (resultSet.next()) {
            val rowId = resultSet.getString("ROWID")
            val scn = resultSet.getLong("ORA_ROWSCN")
            val timestamp = resultSet.getTimestamp("SCN_TIMESTAMP")
            val values = (1 until resultSet.metaData.columnCount - 2).map {
                val name = resultSet.metaData.getColumnName(it)
                val columnDef = schemaDefinition.getColumnSchemaType(name)
                    ?: throw IllegalStateException("Column $name does not exist in schema definition")
                var value = columnDef.extract(it, resultSet)
                if (resultSet.wasNull()) {
                    value = null
                }
                Pair<String, Any?>(name, value)
            }.toMap()
            val cdcRecord = CdcRecord(
                scn = scn,
                username = null,
                timestamp = timestamp,
                transaction = "NOT AVAILABLE",
                table = fetcherOffset.table,
                operation = Operation.READ,
                before = null,
                after = values,
                dataSchema = schemaDefinition,
                rowId = rowId
            )
            val offset = SelectOffset.create(fetcherOffset.asOfScn, fetcherOffset.table, rowId)
            return PollResult(cdcRecord, offset)
        } else {
            null
        }
    }

    fun close() {
        resultSet.close()
        stmt.close()
    }
}