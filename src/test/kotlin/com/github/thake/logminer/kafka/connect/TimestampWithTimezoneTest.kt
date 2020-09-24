package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.SchemaType.TimeType.TimestampType
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

class TimestampWithTimezoneTest {
    @Test
    fun testCorrectlyParsed(){
        val timestamp = "2020-09-24 03:06:31.489000 US/Pacific PDT"
        TimestampType.TimestampWithTimezone(6).convert(timestamp).should{
            it.shouldNotBeNull()
        }
    }
}