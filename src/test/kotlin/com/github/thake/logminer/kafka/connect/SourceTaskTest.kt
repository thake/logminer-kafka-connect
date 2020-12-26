package com.github.thake.logminer.kafka.connect

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
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
import java.util.*

@Testcontainers
class SourceTaskTest : AbstractIntegrationTest() {
    private lateinit var sourceTask: SourceTask
    private lateinit var offsetManager: MockOffsetStorageReader
    private lateinit var defaultConfig: Map<String, String>
    private val log = LoggerFactory.getLogger(SourceTaskTest::class.java)

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
        result.addAll(sourceTask.readAllSourceRecords())
        assertEquals(200, result.size)
    }

    private fun getCurrentScn(conn: Connection): Long {
        @Suppress("SqlResolve")
        return conn.prepareStatement("select CURRENT_SCN from V${'$'}DATABASE").use { stmt ->
            stmt.executeQuery().use {
                it.next()
                it.getLong(1)
            }
        }
    }

    @Test
    fun testInitialScnToCurrentLogSet() {
        val modifyingConnection = openConnection()
        //Initial state
        (0 until 100).forEach { modifyingConnection.insertRow(it) }
        val currentScn = getCurrentScn(modifyingConnection)
        sourceTask.start(
            createConfiguration(
                with(SourceConnectorConfig.Companion) {
                    mapOf(
                        BATCH_SIZE to "10",
                        START_SCN to currentScn.toString()
                    )
                }
            )
        )
        val result = sourceTask.poll().toMutableList()
        assertTrue(result.isEmpty())
        //Now add new rows
        (100 until 200).forEach { modifyingConnection.insertRow(it) }
        //Now continue reading until poll returns an empty list
        result.addAll(sourceTask.readAllSourceRecords())
        assertEquals(100, result.size)
        result.forEach { record ->
            assertEquals(CDC_TYPE, record.sourceOffset()["type"])
        }

    }

    @Test
    fun testInitialScnToOne() {
        val modifyingConnection = openConnection()
        //Initial state
        (0 until 100).forEach { modifyingConnection.insertRow(it) }
        val currentScn = 1L
        sourceTask.start(
            createConfiguration(
                with(SourceConnectorConfig.Companion) {
                    mapOf(
                        BATCH_SIZE to "10",
                        START_SCN to currentScn.toString()
                    )
                }
            )
        )
        val result = sourceTask.readAllSourceRecords().toMutableList()
        assertEquals(100, result.size, "Result does not contain the same size as the number of inserted entries.")
        //Now add new rows
        (100 until 200).forEach { modifyingConnection.insertRow(it) }
        //Now continue reading until poll returns an empty list
        result.addAll(sourceTask.readAllSourceRecords())
        assertEquals(200, result.size)
        result.forEach { record ->
            assertEquals(CDC_TYPE, record.sourceOffset()["type"])
        }

    }

    @Test
    fun testRestartInInitialImport() {
        sourceTask.start(
            createConfiguration(
                mapOf(
                    SourceConnectorConfig.BATCH_SIZE to "10"
                )
            )
        )
        val modifyingConnection = openConnection()
        //Initial state
        (0 until 100).forEach { modifyingConnection.insertRow(it, SECOND_TABLE) }
        val result = sourceTask.poll().toMutableList()

        //Check that the batch size is correct
        assertEquals(10, result.size)
        //Now stop the source
        sourceTask.stop()
        offsetManager.updateOffset(result.last().sourceOffset().toMutableMap())
        sourceTask.start(
            createConfiguration(
                mapOf(
                    SourceConnectorConfig.BATCH_SIZE to "1000"
                )
            )
        )
        //Now add new rows
        (100 until 200).forEach { modifyingConnection.insertRow(it) }
        //Now continue reading until poll returns an empty list
        result.addAll(sourceTask.readAllSourceRecords())
        assertEquals(200, result.size)
    }
    @Test
    fun testResumeDuringCDCAfterDbConnectionLost() {
        sourceTask.start(
            createConfiguration(
                mapOf(
                    SourceConnectorConfig.BATCH_SIZE to "10"
                )
            )
        )
        val modifyingConnection = openConnection()
        //Initial state
        (0 until 10).forEach { modifyingConnection.insertRow(it, SECOND_TABLE) }
        val result = sourceTask.poll().toMutableList()

        //Check that the batch size is correct
        result.shouldHaveSize(10)

        //Now add new rows
        (100 until 200).forEach { modifyingConnection.insertRow(it) }
        //Fetch the next 10 rows. These should be the first cdc rows
        result.addAll(sourceTask.poll())
        result.shouldHaveSize(20)

        log.info("Stopping oracle DB to simulate a lost connection")
        val stopResult = oracle.execInContainer("/bin/bash","-c","service oracle-xe stop")
        log.info("Stop exited with code ${stopResult.exitCode} and log output: ${stopResult.stdout} Err: ${stopResult.stderr}")
        //try to poll now. Should return in an empty result
        val expectedEmptyResult = sourceTask.poll()
        expectedEmptyResult.shouldBeEmpty()
        //Starting again
        val startResult = oracle.execInContainer("/bin/bash", "-c", "service oracle-xe start")
        log.info("Start exited with code ${startResult.exitCode} and log output: ${startResult.stdout} Err: ${startResult.stderr}")

        //Now continue reading until poll returns an empty list
        result.addAll(sourceTask.readAllSourceRecords())
        assertEquals(110, result.size)
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