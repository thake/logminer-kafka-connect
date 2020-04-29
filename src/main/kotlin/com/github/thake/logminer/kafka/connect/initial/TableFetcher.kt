package com.github.thake.logminer.kafka.connect.initial

import com.github.thake.logminer.kafka.connect.*
import java.sql.*

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
            return "SELECT t.*, ROWID, ORA_ROWSCN FROM ${fetcherOffset.table.fullName} AS OF SCN ${fetcherOffset.asOfScn} t $rowIdCondition order by ROWID ASC"
        }
        schemaDefinition = schemaService.getSchema(conn, fetcherOffset.table)
        stmt = conn.prepareStatement(determineQuery())
        try {
            resultSet = stmt.executeQuery()
        } catch (e: SQLException) {
            stmt.close()
            throw e
        }
    }

    fun poll(): PollResult? {
        return try {
            if (resultSet.next()) {
                val rowId = resultSet.getString("ROWID")
                val scn = resultSet.getLong("ORA_ROWSCN")
                val values = (1 until resultSet.metaData.columnCount - 1).map {
                    val name = resultSet.metaData.getColumnName(it)
                    val columnDef = schemaDefinition.getColumnSchemaType(name)
                        ?: throw IllegalStateException("Column $name does not exist in schema definition")
                    var value = try {
                        columnDef.extract(it, resultSet)
                    } catch (e: SQLException) {
                        throw SQLException(
                            "Couldn't convert value of column $name (table: ${fetcherOffset.table.fullName}). Expected type: $columnDef.",
                            e
                        )
                    }
                    if (resultSet.wasNull()) {
                        value = null
                    }
                    Pair<String, Any?>(name, value)
                }.toMap()
                val cdcRecord = CdcRecord(
                    scn = scn,
                    username = null,
                    timestamp = Timestamp(0),
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
        } catch (e: SQLException) {
            close()
            throw e
        }
    }

    fun close() {
        resultSet.close()
        stmt.close()
    }
}