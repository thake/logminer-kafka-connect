package com.github.thake.logminer.kafka.connect

class SourceDatabaseNameService(private val logicalDatabaseName: String) : ConnectNameService {
    private val cachedNames = mutableMapOf<TableId, String>()
    private fun String.sanitizeName(): String {
        fun isValidChar(c: Char) = c == '-' || c == '.' || c == '_' || c in 'A'..'Z' || c in 'a'..'z' || c in '0'..'9'
        val builder = StringBuilder()
        this.forEach {
            builder.append(
                if (!isValidChar(it)) {
                    '_'
                } else {
                    it
                }
            )
        }
        return builder.toString()
    }

    override fun getTopicName(table: TableId) =
        cachedNames.getOrPut(table, { "$logicalDatabaseName.${table.fullName}".sanitizeName() })

    override fun getValueRecordName(table: TableId) = getTopicName(table) + ".Envelope"

    override fun getKeyRecordName(table: TableId) = getTopicName(table) + ".Key"

    override fun getBeforeAfterStructName(table: TableId) = getTopicName(table) + ".Value"
}