package com.github.thake.logminer.kafka.connect.logminer

import com.github.thake.logminer.kafka.connect.Operation
import com.github.thake.logminer.kafka.connect.SchemaDefinition
import com.github.thake.logminer.kafka.connect.SchemaService
import com.github.thake.logminer.kafka.connect.TableId
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import net.openhft.chronicle.queue.ChronicleQueue
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.Timestamp
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread

class TransactionTest {
    val schemaService: SchemaService = mockk()
    val queueFactory: ((xid: String) -> ChronicleQueue) = mockk()
    val conn: Connection = mockk()
    val initialRecord: LogminerRow.Change
    val underTest: Transaction

    init {
        initialRecord = LogminerRow.Change(
                rowIdentifier = LogminerRowIdentifier(1, "1"),
                transaction = "a",
                operation = Operation.INSERT,
                sqlRedo = "Blub",
                table = TableId.ofFullName("blub.blu"),
                timestamp = Timestamp(0),
                username = "ab"
        )
        every {
            schemaService.getSchema(conn, initialRecord.table)
        }.returns(mockk())

        underTest = Transaction(queueFactory, conn, initialRecord, schemaService)
    }

    @Test
    fun testParallelUpdateSchema() {
        val tableId = mockk<TableId>()
        val schemaDefinition = mockk<SchemaDefinition>()
        every {
            schemaService.refreshSchema(conn, tableId)
        }.returns(schemaDefinition)

        val numberOfThreads = 5
        val countDownLatch = CountDownLatch(numberOfThreads)
        repeat(numberOfThreads) {
            thread {
                underTest.updateSchemaIfOutdated(tableId)
                countDownLatch.countDown()
            }
        }
        countDownLatch.await()
        verify(exactly = 1) {
            schemaService.refreshSchema(conn, tableId)
        }

    }
}