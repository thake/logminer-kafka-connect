package com.github.thake.logminer.kafka.connect

import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.*

@Testcontainers
class SourceTaskTest : AbstractIntegrationTest() {
    private lateinit var sourceTask: SourceTask

    private lateinit var defaultConfig: Map<String, String>

    private class TestSourceTaskContext(val configs: Map<String, String>) : SourceTaskContext {

        override fun configs(): MutableMap<String, String> {
            return this.configs.toMutableMap()
        }

        override fun offsetStorageReader(): OffsetStorageReader {
            return MockOffsetStorageReader()
        }

    }

    private class MockOffsetStorageReader : OffsetStorageReader {
        override fun <T : Any?> offsets(partitions: MutableCollection<MutableMap<String, T>>?): MutableMap<MutableMap<String, T>, MutableMap<String, Any>> {
            return Collections.emptyMap()
        }

        override fun <T : Any?> offset(partition: MutableMap<String, T>?): MutableMap<String, Any> {
            return Collections.emptyMap()
        }

    }

    @BeforeEach
    fun setup() {
        defaultConfig =
            with(SourceConnectorConfig.Companion) {
                mapOf(
                    BATCH_SIZE to "1000",
                    TOPIC_PREFIX to "test-",
                    DB_FETCH_SIZE to "10000",
                    DB_SID to oracle.sid,
                    DB_HOST to oracle.containerIpAddress,
                    DB_PORT to oracle.oraclePort.toString(),
                    DB_USERNAME to oracle.username,
                    DB_PASSWORD to oracle.password,
                    START_SCN to "0",
                    MONITORED_TABLES to STANDARD_TABLE.fullName
                )
            }
        sourceTask = SourceTask()
        sourceTask.initialize(TestSourceTaskContext(defaultConfig))
        //Wait for tables to correctly initialize
        Thread.sleep(5000)
    }

    fun createConfiguration(map: Map<String, String>? = null): Map<String, String> {
        return defaultConfig.toMutableMap().apply { map?.let { putAll(it) } }
    }

    @AfterEach
    fun tearDown() {
        sourceTask.stop()
    }

    @Test
    fun testNoScnConfigured() {
        sourceTask.start(
            createConfiguration(
                mapOf(
                    SourceConnectorConfig.BATCH_SIZE to "10"
                )
            )
        )
        val modifyingConnection = openConnection()
        //Initial state
        (0 until 100).forEach { modifyingConnection.insertRow(it) }
        val result = sourceTask.poll().toMutableList()
        assertTrue(result.isNotEmpty())
        //Check that the batch size is correct
        assertEquals(10, result.size)
        //Now add new rows
        (100 until 200).forEach { modifyingConnection.insertRow(it) }
        //Now continue reading until poll returns an empty list
        while (true) {
            val currentResult = sourceTask.poll()
            if (currentResult.isEmpty()) {
                break
            } else {
                result.addAll(currentResult)
            }
        }
        assertEquals(200, result.size)
    }
}