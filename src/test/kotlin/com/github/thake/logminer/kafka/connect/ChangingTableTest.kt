package com.github.thake.logminer.kafka.connect


import com.github.thake.logminer.kafka.connect.logminer.LogminerSchema
import com.github.thake.logminer.kafka.connect.logminer.LogminerSource
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.testcontainers.containers.OracleContainer
import org.testcontainers.ext.ScriptUtils
import org.testcontainers.jdbc.JdbcDatabaseDelegate
import org.testcontainers.junit.jupiter.Container
import java.sql.Connection
import java.time.Duration

class ChangingTableTest : AbstractCdcSourceIntegrationTest() {

    private fun Connection.addOptionalColumnWithDefault(columnName: String, table: TableId = STANDARD_TABLE) {
        this.prepareStatement("alter table ${table.fullName} add $columnName VARCHAR2(255) default 'A'").use {
            it.execute()
        }
    }
    private fun Connection.addNullableColumn(columnName: String, table: TableId = STANDARD_TABLE){
        this.prepareStatement("alter table ${table.fullName} add $columnName VARCHAR2(255)").use {
            it.execute()
        }
    }

    private fun performInsertBeforeChange(conn: Connection, cdcSource: LogminerSource){
        val insertedId = 1
        conn.insertRow(insertedId)
        val results = cdcSource.getResults(conn)
        assertContainsOnlySpecificOperationForIds(results, insertedId.rangeTo(insertedId), Operation.INSERT)
        assertAllAfterColumnsContained(results)
    }

    @ParameterizedTest
    @EnumSource
    fun testAddColumnWithDefaultValue(dictionarySource: LogminerDictionarySource) {
        val conn = openConnection()
        val cdcSource = getCdcSource(dictionarySource)
        performInsertBeforeChange(conn, cdcSource)

        //Now add the column
        val newColumnName = "NEW_COLUMN"
        conn.addOptionalColumnWithDefault(newColumnName)
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
            it.cdcRecord.dataSchema.valueSchema.field(newColumnName).shouldNotBeNull()
            val after = it.cdcRecord.after
            after.shouldNotBeNull()
            after[newColumnName].shouldNotBeNull()
        }
    }
    @ParameterizedTest
    @EnumSource
    fun testAddNullableColumn(dictionarySource: LogminerDictionarySource) {
        val conn = openConnection()
        val cdcSource = getCdcSource(dictionarySource)
        performInsertBeforeChange(conn, cdcSource)

        //Now add the column
        val newColumnName = "NEW_COLUMN"
        conn.addNullableColumn(newColumnName)
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
            after[newColumnName].shouldBeNull()
            it.cdcRecord.dataSchema.valueSchema.field(newColumnName).shouldNotBeNull()
        }
    }
}