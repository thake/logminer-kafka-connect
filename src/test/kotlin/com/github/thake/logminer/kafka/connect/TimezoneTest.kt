package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.initial.SelectSource
import com.github.thake.logminer.kafka.connect.logminer.LogminerConfiguration
import com.github.thake.logminer.kafka.connect.logminer.LogminerSource
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.OracleContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp
import java.time.*
import java.time.temporal.ChronoField
import java.util.*

val TIME_TABLE = TableId(OWNER, "TIME_TEST")
@Testcontainers
class TimezoneTest : AbstractIntegrationTest() {
    private lateinit var selectSource: SelectSource
    private lateinit var cdcSource: LogminerSource
    override fun getInitScript() = "timezoneTest.sql"

    fun getCdcSource(dictionarySource : LogminerDictionarySource = LogminerDictionarySource.ONLINE) : LogminerSource {
        cdcSource = createCdcSource(dictionarySource)
        return cdcSource
    }

    @AfterEach
    fun tearDownCdcSource() {
        cdcSource.stopLogminer()
    }

    private fun createCdcSource(logminerDictionarySource: LogminerDictionarySource, offset: OracleLogOffset = OracleLogOffset.create(0, 0, true)) =
        LogminerSource(
            config = LogminerConfiguration(
                listOf(
                    TableSelector(TIME_TABLE.owner,TIME_TABLE.table)
                ),
                logminerDictionarySource = logminerDictionarySource
            ),
            offset = offset,
            schemaService = SchemaService(SourceDatabaseNameService("A"),defaultZone)
        )
    @BeforeEach
    fun setupSource() {
        //Wait for table creation
        while (!openConnection().metaData.getTables(null, TIME_TABLE.owner, TIME_TABLE.table, null).use {
                it.next()
            }) {
            Thread.sleep(1000)
        }
        Thread.sleep(5000)
        selectSource =
            SelectSource(1000, listOf(TIME_TABLE), SchemaService(SourceDatabaseNameService("A"),defaultZone), null)
    }

    @Test
    fun testCorrectTimestamp(){
        //Set the time zone to a timezone different than the database
        val writeTimezone = TimeZone.getTimeZone("US/Pacific")
        TimeZone.setDefault(writeTimezone)
        val conn = openConnection()
        val timestamp = Timestamp.from(Instant.now())
        conn.prepareStatement("INSERT INTO ${TIME_TABLE.fullName} (id,time,time_with_time_zone,time_with_local_time_zone) VALUES (?,?,?,?)").use {
            it.setInt(1,1)
            it.setTimestamp(2, timestamp)
            it.setTimestamp(3, timestamp)
            it.setTimestamp(4, timestamp)
            it.execute()
        }
        checkSourceReturnsCorrectTimestamp(conn,selectSource,timestamp.time,writeTimezone)
        //Now check the cdcSource
        checkSourceReturnsCorrectTimestamp(conn,getCdcSource(),timestamp.time,writeTimezone)


    }
    private fun checkSourceReturnsCorrectTimestamp(conn : Connection,source : Source<*>, millisSinceEpoch : Long, writeTimezone : TimeZone){
        source.maybeStartQuery(conn)
        val result = source.poll()
        result.size.shouldBe(1)
        val after = result[0].cdcRecord.after
        after.shouldNotBeNull()
        val timeValue = after["TIME"]
        timeValue.shouldBeInstanceOf<java.util.Date>()
        val expectedInstant = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millisSinceEpoch),writeTimezone.toZoneId()).toLocalDateTime().atZone(defaultZone).toInstant()
        timeValue.time.shouldBe(expectedInstant.toEpochMilli())

        val timeWithTimeZone = after["TIME_WITH_TIME_ZONE"]
        timeWithTimeZone.shouldBeInstanceOf<java.util.Date>()
        timeWithTimeZone.time.shouldBe(millisSinceEpoch)
        val timeWithLocalTimeZone = after["TIME_WITH_LOCAL_TIME_ZONE"]
        timeWithLocalTimeZone.shouldBeInstanceOf<java.util.Date>()
        timeWithLocalTimeZone.time.shouldBe(millisSinceEpoch)
    }
}