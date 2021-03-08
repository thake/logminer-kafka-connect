package com.github.thake.logminer.kafka.connect.issues

import com.github.thake.logminer.kafka.connect.*
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.nulls.shouldBeNull
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.Connection
import java.sql.Types
import java.util.*

@Testcontainers
class Issue14Test : AbstractIntegrationTest() {
    private lateinit var sourceTask: SourceTask
    private lateinit var offsetManager: MockOffsetStorageReader
    private lateinit var defaultConfig: Map<String, String>
    private val log = LoggerFactory.getLogger(Issue14Test::class.java)

    private class TestSourceTaskContext(
        val configs: Map<String, String>,
        private val storageReader: OffsetStorageReader = MockOffsetStorageReader()
    ) : SourceTaskContext {

        override fun configs(): MutableMap<String, String> {
            return this.configs.toMutableMap()
        }

        override fun offsetStorageReader(): OffsetStorageReader {
            return storageReader
        }

    }

    private class MockOffsetStorageReader : OffsetStorageReader {
        private var currentOffset = mutableMapOf<String, Any?>()
        fun updateOffset(offset: MutableMap<String, Any?>) {
            currentOffset = offset
        }

        override fun <T : Any?> offsets(partitions: MutableCollection<MutableMap<String, T>>?): MutableMap<MutableMap<String, T>, MutableMap<String, Any>> {
            return Collections.emptyMap()
        }

        override fun <T : Any?> offset(partition: MutableMap<String, T>?): MutableMap<String, Any?> {
            return currentOffset
        }

    }

    @BeforeEach
    fun setup() {
        defaultConfig =
            with(SourceConnectorConfig.Companion) {
                mapOf(
                    BATCH_SIZE to "1000",
                    DB_NAME to "test",
                    DB_FETCH_SIZE to "10000",
                    DB_SID to oracle.sid,
                    DB_HOST to oracle.containerIpAddress,
                    DB_PORT to oracle.oraclePort.toString(),
                    DB_USERNAME to oracle.username,
                    DB_PASSWORD to oracle.password,
                    START_SCN to "0",
                    MONITORED_TABLES to STANDARD_TABLE.fullName + ", " + SECOND_TABLE.fullName
                )
            }
        sourceTask = SourceTask()
        offsetManager = MockOffsetStorageReader()
        sourceTask.initialize(TestSourceTaskContext(defaultConfig, offsetManager))
        //Wait for tables to correctly initialize
        Thread.sleep(5000)
    }

    private fun createConfiguration(map: Map<String, String>? = null): Map<String, String> {
        return defaultConfig.toMutableMap().apply { map?.let { putAll(it) } }
    }

    @AfterEach
    fun tearDown() {
        sourceTask.stop()
    }

    @Test
    fun testUpdateColumnToNull() {
        sourceTask.start(
            createConfiguration(
                mapOf(
                    SourceConnectorConfig.BATCH_SIZE to "10"
                )
            )
        )
        val modifyingConnection = openConnection()
        //Initial state
        (0 until 1).forEach { modifyingConnection.insertRow(it) }
        var result = sourceTask.poll().toMutableList()
        assertTrue(result.isNotEmpty())

        modifyingConnection.prepareStatement("UPDATE ${STANDARD_TABLE.fullName} SET STRING = ?").use { stmt ->
            stmt.setNull(1, Types.NVARCHAR)
            stmt.executeUpdate()
        }

        result = sourceTask.readAllSourceRecords() as MutableList<SourceRecord>
        assertTrue(result.size == 1)
        ((result.get(0).value() as Struct).get("after")as Struct).getString("STRING").shouldBeNull()
    }

    private fun SourceTask.readAllSourceRecords(): List<SourceRecord> {
        val result = mutableListOf<SourceRecord>()
        while (true) {
            val currentResult = poll()
            if (currentResult.isEmpty()) {
                break
            } else {
                result.addAll(currentResult)
            }
        }
        return result
    }
}