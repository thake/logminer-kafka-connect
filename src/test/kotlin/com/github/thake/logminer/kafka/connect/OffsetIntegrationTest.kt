package com.github.thake.logminer.kafka.connect

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
class OffsetIntegrationTest : AbstractCdcSourceIntegrationTest() {
    @Test
    fun testConsecutiveTransactions() {
        val first = openConnection()
        val firstRange = 1..100
        firstRange.forEach { first.insertRow(it) }
        first.close()
        val cdcConn = openConnection()
        val result = cdcSource.getResults(cdcConn)
        assertContainsOnlySpecificOperationForIds(result, firstRange, Operation.INSERT)
        val second = openConnection()
        val secondRange = 101..200
        secondRange.forEach { second.insertRow(it) }
        val secondResult = cdcSource.getResults(cdcConn)
        assertContainsOnlySpecificOperationForIds(secondResult, secondRange, Operation.INSERT)
    }

    @Test
    fun testConcurrentTransactions() {
        val longTransaction = openConnection()
        longTransaction.autoCommit = false
        val shortTransaction = openConnection()
        (1..100).forEach { longTransaction.insertRow(it) }
        shortTransaction.insertRow(101)
        shortTransaction.insertRow(102)
        //Read the first batch before committing the long transaction
        val queryConnection = openConnection()
        val firstBatch = cdcSource.getResults(queryConnection)
        assertContainsOnlySpecificOperationForIds(firstBatch, 101..102, Operation.INSERT)
        //Now commit the long running transaction.
        longTransaction.commit()
        assertContainsOnlySpecificOperationForIds(cdcSource.getResults(queryConnection), 1..100, Operation.INSERT)
    }

    @Test
    fun testRestartConcurrentTransactions() {
        val longTransaction = openConnection()
        longTransaction.autoCommit = false
        val shortTransaction = openConnection()
        shortTransaction.autoCommit = true
        (1..100).forEach { longTransaction.insertRow(it) }
        shortTransaction.insertRow(101)
        shortTransaction.insertRow(102)
        //Read the first batch before committing the long transaction
        val queryConnection = openConnection()
        val firstBatch = cdcSource.getResults(queryConnection)
        assertContainsOnlySpecificOperationForIds(firstBatch, 101..102, Operation.INSERT)
        cdcSource.close()
        queryConnection.close()

        //Now start a new CdcSource with a new connection.
        val newSource = createCdcSource(firstBatch.last().offset as OracleLogOffset)
        val newQueryConnection = openConnection()
        Assertions.assertTrue(
            newSource.getResults(newQueryConnection).isEmpty(),
            "Old transaction records have been read twice!"
        )
        //Now commit the long running transaction.
        longTransaction.commit()
        assertContainsOnlySpecificOperationForIds(newSource.getResults(newQueryConnection), 1..100, Operation.INSERT)
    }

    @Test
    fun testPolledWithinTransaction() {
        val longTransaction = openConnection()
        longTransaction.autoCommit = false
        (1..100).forEach { longTransaction.insertRow(it) }
        //Read the first batch before committing the long transaction
        val queryConnection = openConnection()
        val firstBatch = cdcSource.getResults(queryConnection)
        assertContainsOnlySpecificOperationForIds(firstBatch, IntRange.EMPTY, Operation.INSERT)
        //Write the next entries
        (101..200).forEach { longTransaction.insertRow(it) }
        longTransaction.commit()
        assertContainsOnlySpecificOperationForIds(cdcSource.getResults(queryConnection), 1..200, Operation.INSERT)
    }
}