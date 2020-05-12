package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.logminer.LogminerConfiguration
import com.github.thake.logminer.kafka.connect.logminer.LogminerSource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
abstract class AbstractCdcSourceIntegrationTest : AbstractIntegrationTest() {
    protected lateinit var cdcSource: LogminerSource
    protected open val tableSelector: TableSelector
        get() = TableSelector(OWNER, TABLE_NAME)

    @BeforeEach
    fun setupCdcSource() {
        cdcSource = createCdcSource()
    }

    @AfterEach
    fun tearDownCdcSource() {
        cdcSource.close()
    }

    protected fun createCdcSource(offset: OracleLogOffset = OracleLogOffset.create(0, 0, true)) =
        LogminerSource(
            config = LogminerConfiguration(
                listOf(
                    tableSelector
                )
            ),
            offset = offset,
            schemaService = SchemaService(SourceDatabaseNameService("A"))
        )

}