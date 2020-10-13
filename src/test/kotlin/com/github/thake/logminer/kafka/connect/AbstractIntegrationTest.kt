package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.logminer.LogminerSource
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.Schema
import org.junit.jupiter.api.Assertions.*
import org.testcontainers.containers.OracleContainer
import org.testcontainers.junit.jupiter.Container
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId

enum class Columns {
    ID, TIME, STRING, integer, long, date, BIG_DECIMAL
}

const val OWNER = "SIT"
const val TABLE_NAME = "TEST_TAB"
val STANDARD_TABLE = TableId(OWNER, TABLE_NAME)
val SECOND_TABLE = TableId(OWNER, "SECOND_TAB")

abstract class AbstractIntegrationTest {
    protected open val defaultZone = ZoneId.of("Europe/Berlin")

    @Container
    protected open val oracle: OracleContainer =
        OracleContainer("thake/oracle-xe-11g-archivelog").withInitScript(getInitScript()).withReuse(false)

    protected open fun getInitScript() = "initTestTable.sql"
    fun openConnection(): Connection = oracle.createConnection("")

    protected fun assertContainsOnlySpecificOperationForIds(
        toCheck: List<PollResult>,
        idRange: IntRange,
        operation: Operation,
        table: TableId = STANDARD_TABLE
    ) {
        assertContainsSpecificOperationForIds(toCheck, idRange, operation, table)
        assertEquals(idRange.count(), toCheck.size)
    }

    protected fun assertContainsSpecificOperationForIds(
        toCheck: List<PollResult>,
        idRange: IntRange,
        operation: Operation,
        table: TableId = STANDARD_TABLE
    ) {
        idRange.forEach { id ->
            //Find it in the records
            val record = toCheck.map { it.cdcRecord }.singleOrNull {
                val correctOperationAndName = it.operation == operation && table == it.table
                correctOperationAndName && when (operation) {
                    Operation.READ, Operation.INSERT -> it.after != null && it.before == null && it.after!!["ID"] == id
                    Operation.UPDATE -> it.after != null && it.before != null && it.before!!["ID"] == id
                    Operation.DELETE -> it.after == null && it.before != null && it.before!!["ID"] == id
                }
            }
            assertNotNull(record, "Couldn't find a matching insert row for $id in table $table and operation $operation")

        }

    }

    protected fun LogminerSource.getResults(conn: Connection): List<PollResult> {
        this.maybeStartQuery(conn)
        return this.poll()
    }

    protected fun assertAllBeforeColumnsContained(result: List<PollResult>, columnNames : List<String> = Columns.values().map { it.name }) {
        result.forEach { assertAllColumnsContained(it.cdcRecord.before, columnNames,it.cdcRecord.dataSchema.valueSchema) }
    }

    protected fun assertAllAfterColumnsContained(result: List<PollResult>, columnNames : List<String> = Columns.values().map { it.name }) {
        result.forEach { assertAllColumnsContained(it.cdcRecord.after, columnNames,it.cdcRecord.dataSchema.valueSchema) }
    }


    private fun assertAllColumnsContained(valueMap: Map<String, Any?>?, columnNames : List<String>, expectedSchema : Schema) {
        assertNotNull(valueMap)
        val keys = valueMap!!.keys
        val leftOverKeys = columnNames.toMutableList().apply { removeAll(keys) }
        assertTrue(leftOverKeys.isEmpty(), "Some columns are missing: $leftOverKeys")
        assertEquals(columnNames.size, keys.size)
        assertValuesMatchSchema(valueMap,expectedSchema)
    }
    private fun assertValuesMatchSchema(valueMap: Map<String,Any?>, expectedSchema: Schema){
        expectedSchema.fields().forEach {
            val value = valueMap[it.name()]
            val fieldSchema = it.schema()
            if(fieldSchema.name() == Decimal.LOGICAL_NAME && value != null){
                value.shouldBeInstanceOf<BigDecimal>()
                value.scale().shouldBe(fieldSchema.parameters()[Decimal.SCALE_FIELD]!!.toInt())
            }
        }
    }
}