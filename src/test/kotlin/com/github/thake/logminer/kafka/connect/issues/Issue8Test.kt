package com.github.thake.logminer.kafka.connect.issues

import com.github.thake.logminer.kafka.connect.AbstractIntegrationTest
import com.github.thake.logminer.kafka.connect.OWNER
import com.github.thake.logminer.kafka.connect.SchemaService
import com.github.thake.logminer.kafka.connect.SourceDatabaseNameService
import com.github.thake.logminer.kafka.connect.TableId
import com.github.thake.logminer.kafka.connect.initial.SelectSource
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.comparables.shouldBeEqualComparingTo
import io.kotest.matchers.maps.shouldHaveKeys
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Testcontainers
import java.math.BigDecimal


@Testcontainers
class Issue8Test: AbstractIntegrationTest() {
    private lateinit var selectSource: SelectSource
    companion object{
        val TEST_TABLE = TableId(OWNER, "ID_TEST_TABLE")
    }

    @BeforeEach
    fun setupSource() {
        runScripts("issues/8/idTable.sql")
        //Wait for table creation
        while (!openConnection().metaData.getTables(null, TEST_TABLE.owner, TEST_TABLE.table, null).use {
                it.next()
            }) {
            Thread.sleep(1000)
        }
        Thread.sleep(5000)
        selectSource = SelectSource(1000, listOf(TEST_TABLE), SchemaService(SourceDatabaseNameService("A"),defaultZone), null)

    }

    @AfterEach
    fun destroySource() {
        selectSource.close()
        runScripts("issues/8/dropIdTable.sql")
    }
    @Test
    fun testFullRead(){
        //Insert one entry
        val conn = openConnection()
        val id = 1.toBigDecimal()
        val name = "myTestValue"
        val value = 2.toBigDecimal()

        conn.prepareStatement("INSERT INTO ${TEST_TABLE.fullName} VALUES (?,?,?)").use { stmt ->
            stmt.setBigDecimal(1,id)
            stmt.setString(2,name)
            stmt.setBigDecimal(3,value)
            stmt.executeUpdate()
        }
        selectSource.maybeStartQuery(conn)
        val result = selectSource.poll()
        result.shouldHaveSize(1)

        result[0].cdcRecord.after.should{
            it.shouldNotBeNull()
            it.shouldHaveKeys("ID", "NAME", "VALUE")
            it["ID"].should{idValue ->
                idValue.shouldBeInstanceOf<BigDecimal>()
                idValue.shouldBeEqualComparingTo(id)
            }
            it["NAME"].shouldBe(name)
            it["VALUE"].should{ valueValue ->
                valueValue.shouldBeInstanceOf<BigDecimal>()
                valueValue.shouldBeEqualComparingTo(value)
            }
        }
    }

}