package com.github.thake.logminer.kafka.connect

import mu.KotlinLogging
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.testcontainers.containers.OracleContainer
import org.testcontainers.junit.jupiter.Container
import java.util.concurrent.CountDownLatch

val LOG = KotlinLogging.logger {}

class LongStatementsTest : AbstractCdcSourceIntegrationTest() {
    override val tableSelector: TableSelector
        get() = TableSelector(OWNER, "TAB_WITH_LONG_STMTS")

    @Container
    override val oracle: OracleContainer =
        OracleContainer("thake/oracle-xe-11g-archivelog").withInitScript("initSchemaStatement.sql").withReuse(false)

    @ParameterizedTest
    @EnumSource
    fun testLongStatementWrapping(dictionarySource: LogminerDictionarySource) {
        val columns = 500
        val columnSize = 255
        val strValue = IntRange(0, columnSize - 1).joinToString(separator = "") { "a" }
        val columnRange = IntRange(0, columns)
        val createTableStatement = """create table SIT.TAB_WITH_LONG_STMTS
(
    id NUMBER(8)  constraint TAB_WITH_LONG_STMTS_pk primary key,
""" + columnRange.joinToString(separator = ",\n") { "\tmy_long_column_name_$it  VARCHAR2(${columnSize * 4} CHAR)" } + ")".trim()
        val insertStr =
            "INSERT INTO SIT.TAB_WITH_LONG_STMTS VALUES (?," + columnRange.joinToString(separator = ",") { "?" } + ")"
        openConnection().use {
            it.createStatement().use { stmt ->
                LOG.info { "Creating table with SQL:\n $createTableStatement" }
                stmt.executeUpdate(createTableStatement)
            }
        }
        val entries = 100
        val updateExecutions = 10
        val finished = CountDownLatch(1)
        Thread {
            val conn = openConnection()
            conn.prepareStatement(insertStr).use { stmt ->
                for (i in 0 until entries) {
                    stmt.setInt(1, i)
                    columnRange.forEach {
                        stmt.setString(it + 2, strValue)
                    }
                    stmt.addBatch()
                }
                stmt.executeBatch()
            }
            //Afterwards execute an update
            for (i in 0 until updateExecutions) {
                conn.prepareStatement("UPDATE SIT.TAB_WITH_LONG_STMTS SET my_long_column_name_0 = 'addeeff$i'")
                    .use { it.executeUpdate() }
                LOG.info { "Updated entries #$i" }
            }
            finished.countDown()
        }.start()
        //Wait for the initialization
        Thread.sleep(2000)
        val pollConnection = openConnection()
        var totalReturnedResults = 0
        val cdcSource = getCdcSource(dictionarySource)
        do {
            val results = cdcSource.getResults(pollConnection)
            assertNotNull(results)
            LOG.info { "Retrieved ${results.size} logminer results." }
            totalReturnedResults += results.size
        } while (results.isNotEmpty() || finished.count == 1L)
        assertEquals(totalReturnedResults, (entries * updateExecutions) + entries)
    }
}