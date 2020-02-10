package com.github.thake.logminer.kafka.connect.logminer


import com.github.thake.logminer.kafka.connect.*
import com.github.thake.logminer.sql.parser.LogminerSqlParserUtil
import com.github.thake.logminer.sql.parser.expression.*
import com.github.thake.logminer.sql.parser.expression.operators.relational.EqualsTo
import com.github.thake.logminer.sql.parser.expression.operators.relational.ExpressionList
import com.github.thake.logminer.sql.parser.schema.Column
import com.github.thake.logminer.sql.parser.statement.delete.Delete
import com.github.thake.logminer.sql.parser.statement.insert.Insert
import com.github.thake.logminer.sql.parser.statement.update.Update
import org.apache.kafka.connect.errors.DataException
import java.sql.Timestamp

data class LogminerRowIdentifier(
    val scn: Long,
    val rowId: String
)

sealed class LogminerRow {
    abstract val rowIdentifier: LogminerRowIdentifier
    abstract val transaction: String

    data class Commit(
        override val rowIdentifier: LogminerRowIdentifier,
        override val transaction: String
    ) : LogminerRow()

    data class Rollback(
        override val rowIdentifier: LogminerRowIdentifier,
        override val transaction: String
    ) : LogminerRow()

    data class Change(
        override val rowIdentifier: LogminerRowIdentifier,
        val timestamp: Timestamp,
        override val transaction: String,
        val username: String,
        val table: TableId,
        val sqlRedo: String,
        val operation: Operation
    ) : LogminerRow() {
        private data class ChangeData(val before: Map<String, Any?>?, val after: Map<String, Any?>?)

        fun toCdcRecord(schemaDefinition: SchemaDefinition): CdcRecord {
            val sqlData = parseSql(schemaDefinition, sqlRedo)
            return CdcRecord(
                rowIdentifier.scn,
                rowIdentifier.rowId,
                table,
                timestamp,
                operation,
                transaction,
                username,
                schemaDefinition,
                sqlData.before,
                sqlData.after
            )
        }

        private fun parseSql(
            schemaDefinition: SchemaDefinition,
            sqlRedo: String
        ): ChangeData {
            val stmt = LogminerSqlParserUtil.parse(sqlRedo)
            val dataMap = when (stmt) {
                is Insert -> {
                    stmt.columns.map { extractStringRepresentation(it)!! }
                            .zip((stmt.itemsList as ExpressionList).expressions.map { extractStringRepresentation(it) })
                            .toMap()
                }
                is Update -> {
                    stmt.columns.map { extractStringRepresentation(it)!! }
                            .zip(stmt.expressions.map { extractStringRepresentation(it) }).toMap()
                }
                else -> null
            }
            val beforeDataMap = when (stmt) {
                is Update -> {
                    WhereVisitor()
                            .apply { stmt.where.accept(this) }.before
                }
                is Delete -> {
                    WhereVisitor()
                            .apply { stmt.where.accept(this) }.before
                }
                else -> null
            }
            //Return converted values
            return ChangeData(
                beforeDataMap?.convertToSchemaTypes(schemaDefinition),
                dataMap?.convertToSchemaTypes(schemaDefinition)
            )
        }


        private fun Map<String, String?>.convertToSchemaTypes(schemaDefinition: SchemaDefinition): Map<String, Any?> {
            fun doConvert(schema: SchemaDefinition = schemaDefinition): Map<String, Any?> {
                val map = HashMap<String, Any?>()
                for ((key, value) in this) {
                    val convertedValue = value?.let {
                        convertToSchemaType(
                            it,
                            schema.getColumnSchemaType(key) ?: throw DataException("Column $key does not exist in schema.")
                        )
                    }
                    map[key] = convertedValue
                }
                return map
            }
            return doConvert()
        }

        private fun convertToSchemaType(value: String, schemaType: SchemaType<out Any>) =
            if (value == LogminerSchema.NULL_VALUE) null else schemaType.convert(value)

        private class WhereVisitor : ExpressionVisitorAdapter() {
            val before = mutableMapOf<String, String?>()
            override fun visit(expr: EqualsTo) {
                val columnName = extractStringRepresentation(expr.leftExpression)
                if (columnName != null) {
                    before[columnName] = extractStringRepresentation(expr.rightExpression)
                }
            }
        }
    }

}

private fun extractStringRepresentation(expr: Expression): String? {
    return when (expr) {
        is NullValue -> null
        is DateTimeLiteralExpression -> expr.value
        is StringValue -> expr.value
        is Column -> expr.columnName.removeSurrounding("\"")
        else -> expr.toString()
    }
}