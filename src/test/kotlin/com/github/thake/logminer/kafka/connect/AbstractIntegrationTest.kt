package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.logminer.LogminerSource
import org.junit.jupiter.api.Assertions.*
import org.testcontainers.containers.OracleContainer
import org.testcontainers.junit.jupiter.Container
import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate

enum class Columns {
    ID, TIME, STRING, integer, long, date
}

const val OWNER = "SIT"
const val TABLE_NAME = "TEST_TAB"
val STANDARD_TABLE = TableId(OWNER, TABLE_NAME)
val SECOND_TABLE = TableId(OWNER, "SECOND_TAB")

abstract class AbstractIntegrationTest {

    @Container
    protected val oracle: OracleContainer =
        OracleContainer("thake/oracle-xe-11g-archivelog").withInitScript("InitTestTable.sql").withReuse(false)


    fun openConnection(): Connection = oracle.createConnection("")

    protected fun Connection.executeUpdate(sql: String): Int {
        return this.prepareStatement(sql).use { it.executeUpdate() }
    }

    protected fun Connection.insertRow(id: Int, table: TableId) {
        this.prepareStatement("INSERT INTO ${table.fullName} VALUES (?,?,?,?,?,?)").use {
            it.setInt(1, id)
            it.setTimestamp(2, Timestamp.from(Instant.now()))
            it.setString(3, "Test")
            it.setInt(4, 123456)
            it.setLong(5, 183456L)
            it.setDate(6, Date.valueOf(LocalDate.now()))
            it.execute()
        }
    }

    protected fun Connection.insertRow(id: Int) {
        insertRow(id, STANDARD_TABLE)
    }

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
                    Operation.INSERT -> it.after != null && it.before == null && it.after!!["ID"] == id
                    Operation.UPDATE -> it.after != null && it.before != null && it.before!!["ID"] == id
                    Operation.DELETE -> it.after == null && it.before != null && it.before!!["ID"] == id
                    else -> throw IllegalArgumentException("Operations of state INSERT, UPDATE and DELETE are only supported")
                }
            }
            assertNotNull(record, "Couldn't find a matching insert row for $id in table $table and operation $operation")

        }

    }

    protected fun LogminerSource.getResults(conn: Connection): List<PollResult> {
        this.maybeStartQuery(conn)
        return this.poll()
    }

    protected fun assertAllBeforeColumnsContained(result: List<PollResult>) {
        result.forEach { assertAllColumnsContained(it.cdcRecord.before) }
    }

    protected fun assertAllAfterColumnsContained(result: List<PollResult>) {
        result.forEach { assertAllColumnsContained(it.cdcRecord.after) }
    }

    protected fun assertAllColumnsContained(valueMap: Map<String, Any?>?) {
        assertNotNull(valueMap)
        val upperCaseColumns = Columns.values().map { it.name }
        val keys = valueMap!!.keys
        val leftOverKeys = upperCaseColumns.toMutableList().apply { removeAll(keys) }
        assertTrue(leftOverKeys.isEmpty(), "Some columns are missing: $leftOverKeys")
        assertEquals(upperCaseColumns.size, keys.size)
    }
}