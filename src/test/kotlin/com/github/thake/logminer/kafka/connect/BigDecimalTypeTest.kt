package com.github.thake.logminer.kafka.connect

import io.kotest.matchers.comparables.shouldBeEqualComparingTo
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.sql.ResultSet
import javax.xml.validation.Schema

class BigDecimalTypeTest {
    @Test
    fun testCorrectScaleForString() {
        val type = SchemaType.NumberType.BigDecimalType(13,10)
        val str = "123.20"
        type.convert(str).should {
            it.scale().shouldBe(type.scale)
            it.shouldBeEqualComparingTo(str.toBigDecimal())
        }
    }
    @Test
    fun testCorrectScaleForResultSet(){
        val resultSet = mockk<ResultSet>()
        val columnIndex = 1
        val expectedDecimal = "234.123".toBigDecimal()
        every { resultSet.getBigDecimal(columnIndex) }.returns(expectedDecimal)
        val type = SchemaType.NumberType.BigDecimalType(ORACLE_UNQUALIFIED_NUMBER_PRECISION,
            ORACLE_UNQUALIFIED_NUMBER_SCALE)
        type.extract(columnIndex,resultSet).should {
            it.shouldNotBeNull()
            it.scale().shouldBe(type.scale)
            it.shouldBeEqualComparingTo(expectedDecimal)
        }

    }
}