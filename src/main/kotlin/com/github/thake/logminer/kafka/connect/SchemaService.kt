package com.github.thake.logminer.kafka.connect

import mu.KotlinLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import java.sql.Connection

private val logger = KotlinLogging.logger {}

data class ColumnDefinition(
    val name: String,
    val type: String,
    val scale: Int?,
    val precision: Int,
    val defaultValue: String?,
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
        dbConn.prepareStatement(
            """
            SELECT 
                COLUMN_NAME, 
                DATA_TYPE, 
                DATA_LENGTH, 
                DATA_PRECISION, 
                DATA_SCALE, 
                NULLABLE, 
                DATA_DEFAULT, 
                HIGH_VALUE,
                COMMENTS
            FROM SYS.ALL_TAB_COLUMNS COL LEFT JOIN SYS.ALL_COL_COMMENTS COM USING (COLUMN_NAME,OWNER,TABLE_NAME)
            WHERE OWNER = ? AND TABLE_NAME = ?
        """
        ).apply {
            setString(1, table.owner)
            setString(2, table.table)
        }.use {
            it.executeQuery().use { result ->
                while (result.next()) {
                    val defaultValue = result.getString("DATA_DEFAULT")
                    val name = result.getString("COLUMN_NAME")
                    val precision = result.getInt("DATA_PRECISION")
                    val scale = result.getInt("DATA_SCALE").let { scale ->
                        if (result.wasNull()) {
                            null
                        } else {
                            scale
                        }
                    }
                    val type = result.getString("DATA_TYPE")
                    val doc = result.getString("COMMENTS")
                    val nullable = result.getString("NULLABLE") == "Y"
                    val columnDef = ColumnDefinition(name, type, scale, precision, defaultValue, nullable, doc)
                    val schemaType = SchemaType.toSchemaType(columnDef)
                    columnTypes[name] = schemaType
                    val columnSchema = createColumnSchema(columnDef, schemaType)
                    valueSchemaBuilder.field(name, columnSchema)
                    if (primaryKeys.contains(name)) {
                        keySchemaBuilder.field(name, columnSchema)
                    }
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
        if (column.isNullable || column.defaultValue != null) {
            val defaultValue = if (column.defaultValue == null) {
                null
            } else {
                schemaType.convertDefaultValue(column.defaultValue)
            }
            builder.defaultValue(defaultValue)
        }
        return builder.build()
    }
}