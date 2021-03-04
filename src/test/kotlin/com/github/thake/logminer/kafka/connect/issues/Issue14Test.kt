package com.github.thake.logminer.kafka.connect.issues

import com.github.thake.logminer.kafka.connect.*
import com.github.thake.logminer.kafka.connect.logminer.LogminerSource
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.nulls.shouldNotBeNull
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.sql.Connection
import java.sql.Types


class Issue14Test : AbstractCdcSourceIntegrationTest() {

    private fun performInsertBeforeChange(conn: Connection, cdcSource: LogminerSource){
        val insertedId = 1
        conn.insertRow(insertedId)
        val results = cdcSource.getResults(conn)
        assertContainsOnlySpecificOperationForIds(results, insertedId.rangeTo(insertedId), Operation.INSERT)
        assertAllAfterColumnsContained(results)
    }

    @ParameterizedTest
    @EnumSource
    fun testUpdateColumnToNull(dictionarySource: LogminerDictionarySource) {
        val conn = openConnection()
        val cdcSource = getCdcSource(dictionarySource)

        val insertedId = 1
        conn.insertRow(insertedId)

        conn.prepareStatement("UPDATE ${STANDARD_TABLE.fullName} SET STRING = ?").use { stmt ->
            stmt.setNull(1,Types.NVARCHAR)
            stmt.executeUpdate()
        }
        val results = cdcSource.getResults(conn)
        assertContainsOnlySpecificOperationForIds(results, insertedId.rangeTo(insertedId), Operation.UPDATE)
        assertAllAfterColumnsContained(results)
        results.forEach {
            val after = it.cdcRecord.after
            after.shouldNotBeNull()
            after[Columns.STRING.name].shouldBeNull()
        }
    }
}