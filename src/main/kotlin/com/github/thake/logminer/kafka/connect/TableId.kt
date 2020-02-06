package com.github.thake.logminer.kafka.connect

import java.net.ConnectException

data class TableId(val owner: String, val table: String) {
    val fullName: String = "$owner.$table"
    val recordName: String = getNormalizedTableName(table)

    init {
        if (!table.matches("^[\\w.-_]+$".toRegex())) {
            throw ConnectException("Invalid table name $table for kafka topic.Check table name which must consist only a-z, A-Z, '0-9', ., - and _")
        }
    }

    private fun getNormalizedTableName(tableName: String): String {
        var structName = tableName.substring(0, 1).toUpperCase() + tableName.substring(1).toLowerCase()
        if (structName.endsWith("_t")) {
            structName = structName.substring(0, structName.length - 2)
        }
        return structName
    }

    companion object {
        fun ofFullName(fullname: String): TableId {
            val (owner, table) = fullname.split(".")
            return TableId(owner, table)
        }
    }
}