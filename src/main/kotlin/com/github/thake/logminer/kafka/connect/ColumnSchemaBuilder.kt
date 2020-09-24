package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.SchemaType.TimeType.TimestampType
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Timestamp
import java.math.BigDecimal
import java.sql.ResultSet
import java.time.*
import java.time.format.DateTimeFormatter
import java.util.*

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
    sealed class NumberType<T : Number> : SchemaType<T>() {
        override fun convertDefaultValue(str: String): T? = convert(str.trim())

        object ByteType : NumberType<Byte>() {
            override fun convert(str: String): Byte = str.toByte()
            override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.int8()
            override fun toString(): String = "Byte"
            override fun extract(index: Int, resultSet: ResultSet): Byte = resultSet.getByte(index)
        }

        object ShortType : NumberType<Short>() {
            override fun convert(str: String) = str.toShort()
            override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.int16()
            override fun toString(): String = "Short"
            override fun extract(index: Int, resultSet: ResultSet): Short = resultSet.getShort(index)
        }

        object IntType : NumberType<Int>() {
            override fun convert(str: String) = str.toInt()
            override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.int32()
            override fun toString(): String = "Int"
            override fun extract(index: Int, resultSet: ResultSet): Int = resultSet.getInt(index)
        }

        object LongType : NumberType<Long>() {
            override fun convert(str: String) = str.toLong()
            override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.int64()
            override fun toString(): String = "Long"
            override fun extract(index: Int, resultSet: ResultSet): Long = resultSet.getLong(index)
        }

        object FloatType : NumberType<Float>() {
            override fun convert(str: String) = str.toFloat()
            override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.float32()
            override fun toString(): String = "Float"
            override fun extract(index: Int, resultSet: ResultSet): Float = resultSet.getFloat(index)
        }

        object DoubleType : NumberType<Double>() {
            override fun convert(str: String) = str.toDouble()
            override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.float64()
            override fun toString(): String = "Double"
            override fun extract(index: Int, resultSet: ResultSet): Double = resultSet.getDouble(index)
        }

        data class BigDecimalType(val scale: Int) : NumberType<BigDecimal>() {
            override fun convert(str: String): BigDecimal = str.toBigDecimal().setScale(scale)
            override fun createSchemaBuilder(): SchemaBuilder = Decimal.builder(scale)
            override fun toString(): String = "BigDecimal"
            override fun extract(index: Int, resultSet: ResultSet): BigDecimal? = resultSet.getBigDecimal(index)
                ?.setScale(scale)
        }
    }

    object StringType : SchemaType<String>() {
        override fun convert(str: String) = str
        override fun convertDefaultValue(str: String) = str.trim().removeSurrounding("'")
        override fun createSchemaBuilder(): SchemaBuilder = SchemaBuilder.string()
        override fun toString(): String = "String"
        override fun extract(index: Int, resultSet: ResultSet): String? = resultSet.getString(index)
    }

    sealed class TimeType : SchemaType<java.util.Date>() {
        override fun convertDefaultValue(str: String): java.util.Date? {
            val asUpper = str.trim().toUpperCase()
            return if (UNRESOLVABLE_DATE_TIME_EXPRESSIONS.any { asUpper.contains(it) }) {
                null
            } else {
                convert(cleanDefaultStr(asUpper))
            }
        }

        abstract fun cleanDefaultStr(str: String): String

        object DateType : TimeType() {
            val localDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss]")
            override fun convert(str: String): java.util.Date {
                return java.util.Date.from(
                    LocalDate.parse(
                        str,
                        localDateFormatter
                    ).atStartOfDay().toInstant(ZoneOffset.UTC)
                )
            }

            override fun cleanDefaultStr(str: String) = str.removeSurrounding("DATE '", "'")
            override fun createSchemaBuilder(): SchemaBuilder = Date.builder()
            override fun toString(): String = "Date"
            //Stripping away hours, minutes and seconds
            override fun extract(index: Int, resultSet: ResultSet): java.util.Date? =
                resultSet.getDate(index)?.let {
                    java.util.Date.from(it.toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant())
                }
        }
        abstract class TimestampType(val fractionalSeconds: Int = 6) : TimeType(){
            val fractionalSecondsPart : String
                get() = if(fractionalSeconds > 0) "[.${"S".repeat(fractionalSeconds)}]" else ""

            abstract val pattern : String
            val dateTimeFormatter : DateTimeFormatter by lazy {
                DateTimeFormatter.ofPattern(pattern)
            }
            override fun convert(str: String): java.util.Date = java.util.Date.from(parse(str))
            protected open fun parse(str: String) : Instant = ZonedDateTime.parse(str, dateTimeFormatter).toInstant()
            override fun cleanDefaultStr(str: String) = str.removeSurrounding("TIMESTAMP '", "'")
            override fun createSchemaBuilder(): SchemaBuilder = Timestamp.builder()
            override fun extract(index: Int, resultSet: ResultSet): java.util.Date? =
                resultSet.getTimestamp(index)?.let { java.util.Date(it.time) }

            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (other !is TimestampType) return false

                if (fractionalSeconds != other.fractionalSeconds) return false
                if (pattern != other.pattern) return false

                return true
            }
            override fun hashCode(): Int {
                var result = fractionalSeconds
                result = 31 * result + pattern.hashCode()
                return result
            }

            class TimestampWithoutTimezone(val defaultTimeZone : ZoneId, fractionalSeconds : Int = 6) : TimestampType(fractionalSeconds) {
                override val pattern: String
                    get() = "yyyy-MM-dd HH:mm:ss$fractionalSecondsPart"
                private val cal = Calendar.getInstance(TimeZone.getTimeZone(defaultTimeZone))
                override fun parse(str: String): Instant = LocalDateTime.parse(str, dateTimeFormatter).atZone(defaultTimeZone).toInstant()
                override fun extract(index: Int, resultSet: ResultSet): java.util.Date? =
                    resultSet.getTimestamp(index,cal)?.let { java.util.Date(it.time) }
                override fun toString(): String = "Timestamp($fractionalSeconds) ($defaultTimeZone)"
                override fun equals(other: Any?): Boolean {
                    if (this === other) return true
                    if (other !is TimestampWithoutTimezone) return false
                    if (!super.equals(other)) return false

                    if (defaultTimeZone != other.defaultTimeZone) return false

                    return true
                }
                override fun hashCode(): Int {
                    var result = super.hashCode()
                    result = 31 * result + defaultTimeZone.hashCode()
                    return result
                }
            }
            class TimestampWithTimezone(fractionalSeconds: Int = 6) : TimestampType(fractionalSeconds) {
                //Format:  2020-01-27 06:00:00.640000 US/Pacific PDT
                override val pattern: String
                    get() = "yyyy-MM-dd HH:mm:ss$fractionalSecondsPart VV [zzz]"

                override fun toString(): String = "Timestamp($fractionalSeconds) with timezone"
            }
            class TimestampWithLocalTimezone(fractionalSeconds: Int = 6) : TimestampType(fractionalSeconds){
                //Format:  2020-09-24 10:11:26.684000+00:00
                override val pattern: String
                    get() = "yyyy-MM-dd HH:mm:ss${fractionalSecondsPart}xxx"
                override fun toString(): String = "Timestamp with local timezone"
            }
        }

    }


    companion object {
        fun toSchemaType(columnDataType: ColumnDefinition, defaultZoneId: ZoneId): SchemaType<out Any> {
            val scale = columnDataType.scale
            val precision = columnDataType.precision
            return when (columnDataType.type) {
                "BINARY_FLOAT" -> NumberType.FloatType
                "BINARY_DOUBLE" -> NumberType.DoubleType
                "NUMBER" -> {
                    when {
                        scale == null -> {
                            //Undefined NUMERIC -> Decimal with scale 40 (max digits right of the dot)
                            NumberType.BigDecimalType(40)
                        }
                        precision < 19 -> { // fits in primitive data types.
                            when {
                                scale in NUMERIC_TYPE_SCALE_LOW..0 -> { // integer
                                    when {
                                        precision > 9 -> {
                                            NumberType.LongType
                                        }
                                        precision > 4 -> {
                                            NumberType.IntType
                                        }
                                        precision > 2 -> {
                                            NumberType.ShortType
                                        }
                                        else -> {
                                            NumberType.ByteType
                                        }
                                    }
                                }
                                precision > 0 -> NumberType.DoubleType
                                else ->
                                    NumberType.BigDecimalType(scale)

                            }
                        }
                        else -> {
                            NumberType.BigDecimalType(scale)
                        }
                    }
                }
                "CHAR", "VARCHAR", "VARCHAR2", "NVARCHAR2", "CLOB", "NCLOB", "LONG", "NCHAR" -> StringType
                "DATE" -> TimeType.DateType
                else ->
                    if (columnDataType.type.startsWith("TIMESTAMP")) {
                        val fractionalSeconds = columnDataType.scale ?: 6
                        when {
                            columnDataType.type.endsWith("WITH TIME ZONE") -> TimestampType.TimestampWithTimezone(fractionalSeconds)
                            columnDataType.type.endsWith("WITH LOCAL TIME ZONE") -> TimestampType.TimestampWithLocalTimezone(fractionalSeconds)
                            else -> TimestampType.TimestampWithoutTimezone(defaultZoneId,fractionalSeconds)
                        }
                    } else {
                        throw IllegalArgumentException("Type for column data type $columnDataType is currently not supported")
                    }
            }
        }
    }
}
