package com.github.thake.logminer.kafka.connect

import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Timestamp
import java.math.BigDecimal
import java.sql.ResultSet
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

const val NUMERIC_TYPE_SCALE_LOW = -84
val UNRESOLVABLE_DATE_TIME_EXPRESSIONS = arrayOf(
    "SYSDATE",
    "SYSTIMESTAMP",
    "CURRENT_TIMESTAMP",
    "CURRENT_DATE",
    "LOCALTIMESTAMP"
)
sealed class SchemaType<T> {
    abstract fun createSchemaBuilder(): SchemaBuilder
    abstract fun convert(str: String): T
    abstract fun extract(index: Int, resultSet: ResultSet): T?
    open fun convertDefaultValue(str: String): T? = convert(str)

    object ByteType : SchemaType<Byte>() {
        override fun convert(str: String): Byte = str.toByte()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.int8()
        override fun toString(): String = "Byte"
        override fun extract(index: Int, resultSet: ResultSet): Byte = resultSet.getByte(index)
    }

    object ShortType : SchemaType<Short>() {
        override fun convert(str: String) = str.toShort()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.int16()
        override fun toString(): String = "Short"
        override fun extract(index: Int, resultSet: ResultSet): Short = resultSet.getShort(index)
    }

    object IntType : SchemaType<Int>() {
        override fun convert(str: String) = str.toInt()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.int32()
        override fun toString(): String = "Int"
        override fun extract(index: Int, resultSet: ResultSet): Int = resultSet.getInt(index)
    }

    object LongType : SchemaType<Long>() {
        override fun convert(str: String) = str.toLong()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.int64()
        override fun toString(): String = "Long"
        override fun extract(index: Int, resultSet: ResultSet): Long = resultSet.getLong(index)
    }

    object FloatType : SchemaType<Float>() {
        override fun convert(str: String) = str.toFloat()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.float32()
        override fun toString(): String = "Float"
        override fun extract(index: Int, resultSet: ResultSet): Float = resultSet.getFloat(index)
    }

    object DoubleType : SchemaType<Double>() {
        override fun convert(str: String) = str.toDouble()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.float64()
        override fun toString(): String = "Double"
        override fun extract(index: Int, resultSet: ResultSet): Double = resultSet.getDouble(index)
    }

    data class BigDecimalType(private val scale: Int) : SchemaType<BigDecimal>() {
        override fun convert(str: String): BigDecimal = str.toBigDecimal()
        override fun createSchemaBuilder(): SchemaBuilder = Decimal.builder(scale)
        override fun toString(): String = "BigDecimal"
        override fun extract(index: Int, resultSet: ResultSet): BigDecimal? = resultSet.getBigDecimal(index)
    }

    object StringType : SchemaType<String>() {
        override fun convert(str: String) = str
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.string()
        override fun toString(): String = "String"
        override fun extract(index: Int, resultSet: ResultSet): String? = resultSet.getString(index)
    }


    object DateType : SchemaType<java.util.Date>() {
        val localDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss]")
        override fun convert(str: String): java.util.Date {
            return java.util.Date.from(
                LocalDate.parse(
                    str.removeSurrounding("DATE '", "'"),
                    localDateFormatter
                ).atStartOfDay().toInstant(ZoneOffset.UTC)
            )
        }

        override fun createSchemaBuilder(): SchemaBuilder = Date.builder()
        override fun toString(): String = "Date"
        override fun extract(index: Int, resultSet: ResultSet): java.util.Date? =
            resultSet.getDate(index)?.let { java.util.Date(it.time) }

        override fun convertDefaultValue(str: String): java.util.Date? {
            return if (UNRESOLVABLE_DATE_TIME_EXPRESSIONS.contains(str.toUpperCase())) null else super.convertDefaultValue(
                str
            )
        }
    }

    object TimestampType : SchemaType<java.util.Date>() {
        //Format:  2020-01-27 06:00:00
        val localDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]")

        override fun convert(str: String): java.util.Date {
            return java.util.Date
                    .from(
                        LocalDateTime.parse(str.removeSurrounding("TIMESTAMP '", "'"), localDateTimeFormatter).atZone(
                            ZoneId.systemDefault()
                        ).toInstant()
                    )
        }

        override fun createSchemaBuilder(): SchemaBuilder = Timestamp.builder()
        override fun toString(): String = "Timestamp"

        override fun extract(index: Int, resultSet: ResultSet): java.util.Date? =
            resultSet.getTimestamp(index)?.let { java.util.Date(it.time) }

        override fun convertDefaultValue(str: String): java.util.Date? {
            return if (UNRESOLVABLE_DATE_TIME_EXPRESSIONS.contains(str.toUpperCase())) null else super.convertDefaultValue(
                str
            )
        }
    }


    companion object {
        fun toSchemaType(columnDataType: ColumnDefinition): SchemaType<out Any> {
            //Mapping special oracle values. -127 will be returned as precision if no precision is given (e.g. NUMBER())
            val scale = columnDataType.scale ?: 0

            val precision = columnDataType.precision
            return when (columnDataType.type) {
                "BINARY_FLOAT" -> FloatType
                "BINARY_DOUBLE" -> DoubleType
                "NUMBER" -> {
                    if (precision == 0 && scale == -127) {
                        //Undefined NUMERIC -> Decimal
                        BigDecimalType(0)
                    } else if (precision < 19) { // fits in primitive data types.
                        when {
                            scale in NUMERIC_TYPE_SCALE_LOW..0 -> { // integer
                                when {
                                    precision > 9 -> {
                                        LongType
                                    }
                                    precision > 4 -> {
                                        IntType
                                    }
                                    precision > 2 -> {
                                        ShortType
                                    }
                                    else -> {
                                        ByteType
                                    }
                                }
                            }
                            precision > 0 -> DoubleType
                            else ->
                                BigDecimalType(scale)

                        }
                    } else {
                        BigDecimalType(scale)
                    }
                }
                "CHAR", "VARCHAR", "VARCHAR2", "NVARCHAR2", "CLOB", "NCLOB", "LONG", "NCHAR" -> StringType
                "DATE" -> DateType
                else ->
                    if (columnDataType.type.startsWith("TIMESTAMP")) {
                        TimestampType
                    } else {
                        throw IllegalArgumentException("Type for column data type $columnDataType is currently not supported")
                    }
            }
        }
    }
}
