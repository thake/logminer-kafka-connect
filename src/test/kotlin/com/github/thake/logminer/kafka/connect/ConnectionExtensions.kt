package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.logminer.LogminerSchema
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Date
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate


fun Connection.executeUpdate(sql: String): Int {
    return this.prepareStatement(sql).use { it.executeUpdate() }
}
val Connection.currentScn
    get() = this.prepareStatement("Select CURRENT_SCN from v${"$"}database").use { statement ->
        statement.executeQuery().use {
            it.next()
            it.getLong(1)
        }
    }
fun Connection.insertRow(id: Int, table: TableId) {
    val columnList = Columns.values().joinToString(",", "(", ")") { "\"${it.name}\"" }
    LOG.info { "SCN before inserting row with id $id in $table: ${this.currentScn}" }
    this.prepareStatement("INSERT INTO ${table.fullName} $columnList VALUES (?,?,?,?,?,?,?)").use {
        it.setInt(1, id)
        it.setTimestamp(2, Timestamp.from(Instant.now()))
        it.setString(3, "Test")
        it.setInt(4, 123456)
        it.setLong(5, 183456L)
        it.setDate(6, Date.valueOf(LocalDate.now()))
        it.setBigDecimal(7, BigDecimal("30.516658782958984"))
        val result = it.executeUpdate()
        if(result == 0){
            throw IllegalStateException("Could not insert row with id $id")
        }else{
            LOG.info { "Inserted new row in $table with id $id" }
        }
    }
    LOG.info {"SCN for inserted row with id $id in $table: ${this.getScnOfRow(id,table)}"}
    LOG.info { "SCN after inserting row with id $id in $table: ${this.currentScn}" }
}
fun Connection.getScnOfRow(id : Int, table: TableId) : Long{
    return this.prepareStatement("SELECT ORA_ROWSCN FROM ${table.fullName} WHERE id = ?").use { stmt ->
        stmt.setInt(1,id)
        stmt.executeQuery().use { resultSet ->
            resultSet.next()
            resultSet.getLong(1)
        }
    }
}

fun Connection.insertRow(id: Int) {
    insertRow(id, STANDARD_TABLE)
}