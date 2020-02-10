package com.github.thake.logminer.kafka.connect

import mu.KotlinLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import java.sql.Connection
import java.sql.DatabaseMetaData

private val logger = KotlinLogging.logger {}

data class ColumnDefinition(
    val name: String,
    val type: Int,
    val size: Int,
    val precision: Int?,
    val isNullable: Boolean,
    val doc: String?
)

data class SchemaDefinition(
    val table: TableId,
    val valueSchema: Schema,
    val keySchema: Schema,
    private val columnTypes: Map<String, SchemaType<out Any>>
) {
    fun getColumnSchemaType(columnName: String) = columnTypes[columnName]
}

class SchemaService(private val nameService: ConnectNameService) {
    private val cachedSchemas: MutableMap<TableId, SchemaDefinition> = mutableMapOf()

    fun getSchema(dbConn: Connection, table: TableId) = cachedSchemas.getOrPut(table, { buildTableSchema(dbConn, table) })
    fun refreshSchema(dbConn: Connection, table: TableId): SchemaDefinition {
        cachedSchemas.remove(table)
        return getSchema(dbConn, table)
    }

    private fun buildTableSchema(dbConn: Connection, table: TableId): SchemaDefinition {
        logger.info { "Getting dictionary details for table : $table" }

        val valueSchemaBuilder =
            SchemaBuilder.struct().name(nameService.getBeforeAfterStructName(table))
        val keySchemaBuilder = SchemaBuilder.struct().name(nameService.getKeyRecordName(table))
        val columnTypes = mutableMapOf<String, SchemaType<out Any>>()
        val primaryKeys = mutableSetOf<String>()
        dbConn.metaData.getPrimaryKeys(null, table.owner, table.table).use {
            while (it.next()) {
                primaryKeys.add(it.getString(4))
            }
        }
        dbConn.metaData.getColumns(null, table.owner, table.table, null).use {
            while (it.next()) {
                val name = it.getString(4)
                val precision = it.getInt(9)
                val size = it.getInt(7)
                val type = it.getInt(5)
                val doc = it.getString(12)
                val nullable = it.getInt(11) != DatabaseMetaData.columnNoNulls
                val columnDef = ColumnDefinition(name, type, size, precision, nullable, doc)
                val schemaType = SchemaType.toSchemaType(columnDef)
                columnTypes[name] = schemaType
                val columnSchema = createColumnSchema(columnDef, schemaType)
                valueSchemaBuilder.field(name, columnSchema)
                if (primaryKeys.contains(name)) {
                    keySchemaBuilder.field(name, columnSchema)
                }
            }
        }


        return SchemaDefinition(
            table,
            valueSchemaBuilder.optional().build(),
            keySchemaBuilder.optional().build(),
            columnTypes
        )
    }

    private fun createColumnSchema(column: ColumnDefinition, schemaType: SchemaType<out Any>): Schema {
        val builder = schemaType.createSchemaBuilder()
        if (column.isNullable) {
            builder.optional()
        }
        if (column.doc != null) {
            builder.doc(column.doc)
        }
        return builder.build()
    }
}