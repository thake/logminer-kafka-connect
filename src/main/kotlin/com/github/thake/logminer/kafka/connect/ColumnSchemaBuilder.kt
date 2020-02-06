package com.github.thake.logminer.kafka.connect

import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Timestamp
import java.math.BigDecimal
import java.sql.Types
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

const val NUMERIC_TYPE_SCALE_LOW = -84

sealed class SchemaType<T> {
    abstract fun createSchemaBuilder(): SchemaBuilder
    abstract fun convert(str: String): T

    object BooleanType : SchemaType<Boolean>() {
        override fun convert(str: String): Boolean = str.toBoolean()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.bool()
        override fun toString(): String = "Boolean"
    }

    object ByteType : SchemaType<Byte>() {
        override fun convert(str: String): Byte = str.toByte()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.int8()
        override fun toString(): String = "Byte"
    }

    object ShortType : SchemaType<Short>() {
        override fun convert(str: String) = str.toShort()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.int16()
        override fun toString(): String = "Short"
    }

    object IntType : SchemaType<Int>() {
        override fun convert(str: String) = str.toInt()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.int32()
        override fun toString(): String = "Int"
    }

    object LongType : SchemaType<Long>() {
        override fun convert(str: String) = str.toLong()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.int64()
        override fun toString(): String = "Long"
    }

    object FloatType : SchemaType<Float>() {
        override fun convert(str: String) = str.toFloat()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.float32()
        override fun toString(): String = "Float"
    }

    object DoubleType : SchemaType<Double>() {
        override fun convert(str: String) = str.toDouble()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.float64()
        override fun toString(): String = "Double"
    }

    class BigDecimalType(private val scale: Int) : SchemaType<BigDecimal>() {
        override fun convert(str: String): BigDecimal = str.toBigDecimal()
        override fun createSchemaBuilder(): SchemaBuilder = Decimal.builder(scale)
        override fun toString(): String = "BigDecimal"
    }

    object StringType : SchemaType<String>() {
        override fun convert(str: String) = str
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.string()
        override fun toString(): String = "String"
    }

    object BinaryType : SchemaType<ByteArray>() {
        override fun convert(str: String) = str.toByteArray()
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.bytes()
        override fun toString(): String = "Binary"
    }

    object DateType : SchemaType<java.util.Date>() {
        val localDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        override fun convert(str: String): java.util.Date {
            return java.util.Date.from(LocalDate.parse(str, localDateFormatter).atStartOfDay().toInstant(ZoneOffset.UTC))
        }

        override fun createSchemaBuilder(): SchemaBuilder = Date.builder()
        override fun toString(): String = "Date"
    }

    object TimestampType : SchemaType<java.util.Date>() {
        //Format:  2020-01-27 06:00:00
        val localDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]")

        override fun convert(str: String): java.util.Date {
            return java.util.Date
                    .from(LocalDateTime.parse(str, localDateTimeFormatter).atZone(ZoneId.systemDefault()).toInstant())
        }

        override fun createSchemaBuilder(): SchemaBuilder = Timestamp.builder()
        override fun toString(): String = "Timestamp"
    }


    companion object {
        fun toSchemaType(columnDataType: ColumnDefinition): SchemaType<out Any> {
            //Mapping special oracle values. -127 will be returned as precision if no precision is given (e.g. NUMBER())
            val precision = columnDataType.precision ?: 0

            val size = columnDataType.size
            return when (columnDataType.type) {
                Types.BOOLEAN -> BooleanType
                Types.BIT -> ByteType
                Types.TINYINT -> IntType
                Types.SMALLINT -> IntType
                Types.INTEGER -> LongType
                Types.BIGINT -> LongType
                Types.REAL -> FloatType
                Types.FLOAT, Types.DOUBLE -> DoubleType
                Types.NUMERIC -> {
                    if (size == 0 && precision == -127) {
                        //Undefined NUMERIC -> Decimal
                        BigDecimalType(0)
                    } else if (size < 19) { // fits in primitive data types.
                        when {
                            precision in NUMERIC_TYPE_SCALE_LOW..0 -> { // integer
                                when {
                                    size > 9 -> {
                                        LongType
                                    }
                                    size > 4 -> {
                                        IntType
                                    }
                                    size > 2 -> {
                                        ShortType
                                    }
                                    else -> {
                                        ByteType
                                    }
                                }
                            }
                            size > 0 -> DoubleType
                            else ->
                                BigDecimalType(precision)

                        }
                    } else {
                        BigDecimalType(precision)
                    }
                }
                Types.DECIMAL -> BigDecimalType(precision)
                Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR, Types.CLOB, Types.NCLOB, Types.DATALINK, Types.SQLXML -> StringType
                Types.BINARY, Types.BLOB, Types.VARBINARY, Types.LONGVARBINARY -> BinaryType
                Types.DATE -> DateType
                Types.TIMESTAMP -> TimestampType
                else -> throw IllegalArgumentException("Type for column data type $columnDataType is currently not supported")
            }
        }
    }
}
