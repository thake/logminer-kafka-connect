package com.github.thake.logminer.kafka.connect

interface ConnectNameService {
    fun getTopicName(table: TableId): String
    fun getValueRecordName(table: TableId): String
    fun getKeyRecordName(table: TableId): String
    fun getBeforeAfterStructName(table: TableId): String
}