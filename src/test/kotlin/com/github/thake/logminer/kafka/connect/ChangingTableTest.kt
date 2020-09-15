package com.github.thake.logminer.kafka.connect


import io.kotest.matchers.nulls.shouldNotBeNull
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.sql.Connection

class ChangingTableTest : AbstractCdcSourceIntegrationTest() {
    private fun Connection.addOptionalColumn(columnName: String, table: TableId = STANDARD_TABLE) {
        this.prepareStatement("alter table ${table.fullName} add $columnName VARCHAR2(255) default 'A'").use {
            it.execute()
        }
    }

    @ParameterizedTest
    @EnumSource
    fun testInsertRecord(dictionarySource: LogminerDictionarySource) {
        val conn = openConnection()
        val insertedId = 1
        conn.insertRow(insertedId)
        val cdcSource = getCdcSource(dictionarySource)
        val results = cdcSource.getResults(conn)
        assertContainsOnlySpecificOperationForIds(results, insertedId.rangeTo(insertedId), Operation.INSERT)
        assertAllAfterColumnsContained(results)

        //Now add the column
        val newColumnName = "NEW_COLUMN"
        conn.addOptionalColumn(newColumnName)
        val newInsertedId = 2
        conn.insertRow(newInsertedId)
        val resultsWithNewColumn = cdcSource.getResults(conn)
        assertContainsOnlySpecificOperationForIds(
            resultsWithNewColumn,
            newInsertedId.rangeTo(newInsertedId),
            Operation.INSERT
        )
        assertAllAfterColumnsContained(resultsWithNewColumn, Columns.values().map { it.name }.plus(newColumnName))
        resultsWithNewColumn.forEach {
            val after = it.cdcRecord.after
            after.shouldNotBeNull()
            after[newColumnName].shouldNotBeNull()
        }

    }
}