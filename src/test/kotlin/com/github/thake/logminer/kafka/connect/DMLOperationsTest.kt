package com.github.thake.logminer.kafka.connect

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.*

@Testcontainers
class DMLOperationsTest : AbstractCdcSourceIntegrationTest() {

    @Test
    fun testInsertRecord() {
        val conn = openConnection()
        val insertedId = 1
        conn.insertRow(1)
        val results = cdcSource.getResults(conn)
        assertContainsOnlySpecificOperationForIds(results, insertedId.rangeTo(insertedId), Operation.INSERT)
        assertAllAfterColumnsContained(results)
    }

    @Test
    fun testDeleteRecord() {
        val conn = openConnection()
        (0 until 100).forEach { conn.insertRow(it) }
        //Clear results by explicitly polling them
        cdcSource.getResults(conn)
        conn.executeUpdate("DELETE FROM ${STANDARD_TABLE.fullName} WHERE id < 50")
        val results = cdcSource.getResults(conn)
        assertContainsOnlySpecificOperationForIds(results, 0 until 50, Operation.DELETE)
        assertAllBeforeColumnsContained(results)
    }

    @Test
    fun testUpdateRecords() {
        val conn = openConnection()
        (0 until 100).forEach { conn.insertRow(it) }
        cdcSource.getResults(conn)
        conn
                .executeUpdate("UPDATE ${STANDARD_TABLE.fullName} SET string = 'AAAA', time = TIMESTAMP '2020-01-13 15:45:01', \"date\" = DATE '2020-01-13' where id < 50")
        val results = cdcSource.getResults(conn)
        assertContainsOnlySpecificOperationForIds(results, 0 until 50, Operation.UPDATE)
        assertAllBeforeColumnsContained(results)
        results.forEach {
            val after = it.cdcRecord.after!!
            assertEquals(3, after.size)
            assertEquals("AAAA", after[Columns.STRING.name])
            assertEquals(
                Date.from(LocalDateTime.of(2020, 1, 13, 15, 45, 1).atZone(ZoneId.systemDefault()).toInstant()),
                after[Columns.TIME.name]
            )
            assertEquals(
                Date.from(LocalDate.of(2020, 1, 13).atStartOfDay(ZoneOffset.UTC).toInstant()),
                after[Columns.date.name]
            )
        }
    }

}