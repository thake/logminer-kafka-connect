package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.initial.SelectSource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class SelectSourceTest : AbstractIntegrationTest() {
    private lateinit var selectSource: SelectSource
    @BeforeEach
    fun setupSource() {
        //Wait for table creation
        while (!openConnection().metaData.getTables(null, STANDARD_TABLE.owner, STANDARD_TABLE.table, null).use {
                it.next()
            }) {
            Thread.sleep(1000)
        }
        Thread.sleep(5000)
        selectSource = SelectSource(1000, listOf(STANDARD_TABLE, SECOND_TABLE), SchemaService(), null)

    }

    @AfterEach
    fun destroySource() {
        selectSource.close()
    }

    @Test
    fun checkSingleTable() {
        val conn = openConnection()
        (0 until 100).forEach { conn.insertRow(it) }
        selectSource.maybeStartQuery(conn)
        val result = selectSource.poll()
        assertContainsOnlySpecificOperationForIds(result, 0 until 100, Operation.INSERT)
        assertNotNull(selectSource.getOffset())
        selectSource.maybeStartQuery(conn)
        val emptyResult = selectSource.poll()
        assertNotNull(emptyResult)
        assertTrue(emptyResult.isEmpty())
    }

    @Test
    fun checkEmptySingleTable() {
        val conn = openConnection()
        selectSource.maybeStartQuery(conn)
        val result = selectSource.poll()
        assertNotNull(result)
        assertTrue(result.isEmpty())
        assertNull(selectSource.getOffset())
    }

    @Test
    fun checkMultipleTables() {
        val conn = openConnection()
        (0 until 100).forEach { conn.insertRow(it, STANDARD_TABLE) }
        (0 until 100).forEach { conn.insertRow(it, SECOND_TABLE) }
        selectSource.maybeStartQuery(conn)
        val result = selectSource.poll()
        assertContainsSpecificOperationForIds(result, 0 until 100, Operation.INSERT, STANDARD_TABLE)
        assertContainsSpecificOperationForIds(result, 0 until 100, Operation.INSERT, SECOND_TABLE)
        assertEquals(200, result.size)
        assertNotNull(selectSource.getOffset())
        selectSource.maybeStartQuery(conn)
        val emptyResult = selectSource.poll()
        assertNotNull(emptyResult)
        assertTrue(emptyResult.isEmpty())
    }

    @Test
    fun checkNoDirtyReads() {
        selectSource = SelectSource(10, listOf(STANDARD_TABLE), SchemaService(), null)
        val conn = openConnection()
        (0 until 100).forEach { conn.insertRow(it) }
        selectSource.maybeStartQuery(conn)
        val result = selectSource.poll()
        selectSource.close()
        val dirtyWriteTransaction = openConnection()
        (100 until 200).forEach { dirtyWriteTransaction.insertRow(it) }
        selectSource = SelectSource(1000, listOf(STANDARD_TABLE), SchemaService(), selectSource.lastOffset)
        selectSource.maybeStartQuery(openConnection())
        val secondResult = selectSource.poll()
        val totalResult = result.toMutableList().apply { addAll(secondResult) }
        //Committed rows of dirtyWriteTransaction should not be included in the result set.
        assertContainsOnlySpecificOperationForIds(totalResult, 0 until 100, Operation.INSERT)
        assertNotNull(selectSource.getOffset())
        selectSource.maybeStartQuery(conn)
        val emptyResult = selectSource.poll()
        assertNotNull(emptyResult)
        assertTrue(emptyResult.isEmpty())
    }

}