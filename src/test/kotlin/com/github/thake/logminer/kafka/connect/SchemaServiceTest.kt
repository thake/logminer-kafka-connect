package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.SchemaType.NumberType.*
import com.github.thake.logminer.kafka.connect.SchemaType.StringType
import com.github.thake.logminer.kafka.connect.SchemaType.TimeType.DateType
import com.github.thake.logminer.kafka.connect.SchemaType.TimeType.TimestampType
import io.kotlintest.*
import io.kotlintest.matchers.types.shouldNotBeNull
import io.kotlintest.specs.WordSpec
import org.testcontainers.containers.OracleContainer
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

class SchemaServiceTest : WordSpec() {

    val oracle: OracleContainer =
        OracleContainer("thake/oracle-xe-11g-archivelog").withInitScript("InitTestTable.sql").withReuse(false)
    var table = TableId("SIT", "MY_SCHEMA_TEST_TABLE")
    val columnName = "A"
    lateinit var connection: Connection
    lateinit var schemaService: SchemaService
    private fun createTable(columnDef: String, comment: String?) {
        connection.prepareCall("CREATE TABLE ${table.fullName} ($columnName $columnDef)").use { it.execute() }
        if (comment != null) {
            connection.prepareCall("COMMENT ON COLUMN ${table.fullName}.$columnName IS '$comment'").use { it.execute() }
        }

    }

    override fun isolationMode(): IsolationMode? = IsolationMode.SingleInstance
    override fun beforeTest(testCase: TestCase) {
        connection = DriverManager.getConnection(oracle.jdbcUrl)
        connection.prepareCall("DROP TABLE ${table.fullName}").use {
            try {
                it.execute()
            } catch (e: SQLException) {
                //Ignore exception
            }
        }
        schemaService = SchemaService(SourceDatabaseNameService("test"))
        table = TableId(table.owner, "MY_${testCase.getLine()}")

    }

    override fun beforeSpec(spec: Spec) {
        oracle.start()
    }

    override fun afterSpec(spec: Spec) {
        oracle.stop()
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        connection.close()
    }

    fun <T> String.shouldBe(
        schemaType: SchemaType<T>,
        nullable: Boolean = true,
        defaultValue: T? = null,
        comment: String? = null
    ) {
        createTable(this, comment)
        val schemaDef = schemaService.getSchema(connection, table)
        schemaDef.shouldNotBeNull()
        schemaDef.table.shouldBe(table)
        val columnDef = schemaDef.getColumnSchemaType(columnName)
        columnDef.shouldBe(schemaType)
        val field = schemaDef.valueSchema.field(columnName)
        field.shouldNotBeNull()
        val schema = field.schema()
        schema.isOptional.shouldBe(nullable)
        schema.defaultValue().shouldBe(defaultValue)
        schema.doc().shouldBe(comment)
    }

    init {
        "test correct types" should {
            "byte" {
                "NUMBER(2,0)".shouldBe(ByteType)
            }
            "short" {
                "NUMBER(3,0)".shouldBe(ShortType)
            }
            "shortWithoutPrecision" {
                "NUMBER(4)".shouldBe(ShortType)
            }
            "int"{
                "NUMBER(5,0)".shouldBe(IntType)
            }
            "long"{
                "NUMBER(10,0)".shouldBe(LongType)
            }
            "BigDecimal"{
                "NUMBER(20,0)".shouldBe(BigDecimalType(0))
            }
            "undefined NUMBER"{
                "NUMBER".shouldBe(BigDecimalType(0))
            }
            "Date"{
                "DATE".shouldBe(DateType)
            }
            "Timestamp"{
                "TIMESTAMP".shouldBe(TimestampType)
            }
            "Timestamp with timezone"{
                "TIMESTAMP WITH TIME ZONE".shouldBe(TimestampType)
            }
            "Timestamp with local timezone"{
                "TIMESTAMP WITH LOCAL TIME ZONE".shouldBe(TimestampType)
            }
            "byteDefault" {
                "NUMBER(2,0) default 1".shouldBe(ByteType, true, 1.toByte())
            }
            "shortDefault" {
                "NUMBER(3,0) default 1".shouldBe(ShortType, true, 1.toShort())
            }
            "intDefault"{
                "NUMBER(5,0) default 1".shouldBe(IntType, true, 1)
            }
            "longDefault"{
                "NUMBER(10,0) default 1".shouldBe(LongType, true, 1L)
            }
            "long default with space"{
                "NUMBER(10,0) default 1 ".shouldBe(LongType, true, 1L)
            }
            "BigDecimalDefault"{
                "NUMBER(20,0) default 1".shouldBe(BigDecimalType(0), true, BigDecimal.ONE)
            }
            "date small"{
                "date".shouldBe(DateType)
            }
            "DateDefault"{
                "DATE default DATE '2018-04-12'".shouldBe(
                    DateType,
                    true,
                    java.util.Date.from(LocalDate.of(2018, 4, 12).atStartOfDay(ZoneOffset.UTC).toInstant())
                )
            }
            "TimestampDefault"{
                "TIMESTAMP default TIMESTAMP '2018-04-12 01:00:00'".shouldBe(
                    TimestampType, true, Timestamp.valueOf(
                        LocalDateTime.of(2018, 4, 12, 1, 0, 0)
                    )
                )
            }

            "TimestampCurrentTimestampDefault"{
                "TIMESTAMP default current_timestamp".shouldBe(TimestampType, true)
            }
            "Timestamp sysdate default with space"{
                "TIMESTAMP default sysdate ".shouldBe(TimestampType, true)
            }
            "markedAsNullable"{
                "NUMBER(10,0)".shouldBe(LongType, true)
            }
            "markesAsNonNullable"{
                "NUMBER(10,0) not null".shouldBe(LongType, false)
            }
            "hasComment"{
                "NUMBER(10,0)".shouldBe(LongType, true, null, "My Comment")
            }
            "varchar2"{
                "VARCHAR2(5 CHAR)".shouldBe(StringType)
            }
            "char"{
                "CHAR".shouldBe(StringType)
            }
            "nchar"{
                "NCHAR".shouldBe(StringType)
            }
            "nvarchar2"{
                "NVARCHAR2(5)".shouldBe(StringType)
            }
            "varchar2 default"{
                "VARCHAR2(5 CHAR) default 'N'".shouldBe(StringType, true, "N")
            }
            "non parsable default"{
                "NUMBER(5) default 1+2".shouldBe(IntType)
            }
        }
    }
}