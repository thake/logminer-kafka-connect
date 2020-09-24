package com.github.thake.logminer.kafka.connect

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.ZoneId

@Testcontainers
class SourceConnectorConfigTest : AbstractIntegrationTest() {
    override fun getInitScript() = "timezoneTest.sql"
    private val underTest by lazy {
        SourceConnectorConfig(with(SourceConnectorConfig.Companion) {
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
        })
    }
    @Test
    fun testTimezoneSetCorrectly(){
        underTest.dbZoneId.shouldBe(ZoneId.of("UTC"))
    }

}