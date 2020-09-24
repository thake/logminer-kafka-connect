package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.logminer.LogminerConfiguration
import com.github.thake.logminer.kafka.connect.logminer.LogminerSource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.ZoneId

@Testcontainers
abstract class AbstractCdcSourceIntegrationTest : AbstractIntegrationTest() {
    private lateinit var cdcSource: LogminerSource

    protected open val tableSelector: TableSelector
        get() = TableSelector(OWNER, TABLE_NAME)


    fun getCdcSource(dictionarySource : LogminerDictionarySource) : LogminerSource {
        cdcSource = createCdcSource(dictionarySource)
        return cdcSource
    }

    @AfterEach
    fun tearDownCdcSource() {
        cdcSource.stopLogminer()
    }

    protected fun createCdcSource(logminerDictionarySource: LogminerDictionarySource, offset: OracleLogOffset = OracleLogOffset.create(0, 0, true)) =
        LogminerSource(
            config = LogminerConfiguration(
                listOf(
                    tableSelector
                ),
                logminerDictionarySource = logminerDictionarySource
            ),
            offset = offset,
            schemaService = SchemaService(SourceDatabaseNameService("A"),defaultZone)
        )
}