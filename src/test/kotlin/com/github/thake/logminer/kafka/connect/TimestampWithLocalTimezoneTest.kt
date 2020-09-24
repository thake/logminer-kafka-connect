package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.SchemaType.TimeType.TimestampType
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.*

class TimestampWithLocalTimezoneTest {
    @Test
    fun testCorrectlyParsed(){
        val timestamp = "2020-09-24 10:11:26.684000+00:00"
        TimestampType.TimestampWithLocalTimezone(6).convert(timestamp).should{
            it.shouldNotBeNull()
            it.shouldBe(Date.from(ZonedDateTime.of(2020,9,24,10,11,26, Duration.ofMillis(684).nano
                ,ZoneId.of("UTC")).toInstant()))
        }
    }
}