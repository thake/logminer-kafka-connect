package com.github.thake.logminer.kafka.connect

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance

sealed class LogMinerSelector
data class TableSelector(val owner: String, val tableName: String) : LogMinerSelector()
data class SchemaSelector(val owner: String) : LogMinerSelector()
class SourceConnectorConfig(
    config: ConfigDef?,
    parsedConfig: Map<String, String>
) : AbstractConfig(config, parsedConfig) {
    constructor(parsedConfig: Map<String, String>) : this(
        conf(),
        parsedConfig
    )


    val dbName: String
        get() = getString(DB_SID)

    val dbHostName: String
        get() = getString(DB_HOST)

    val dbPort: Int
        get() = getInt(DB_PORT)

    val dbUser: String
        get() = getString(DB_USERNAME)

    val dbPassword: String
        get() = getString(DB_PASSWORD)

    val topicPrefix: String
        get() = getString(TOPIC_PREFIX)

    val whitelistedTables: List<String>
        get() = getString(MONITORED_TABLES).split(",").map { it.trim() }

    val logMinerSelectors: List<LogMinerSelector>
        get() = whitelistedTables.map {
            val parts = it.split(".")
            if (parts.size > 1) {
                TableSelector(parts[0], parts[1])
            } else {
                SchemaSelector(parts[0])
            }
        }

    val batchSize: Int
        get() = getInt(BATCH_SIZE)
    val dbFetchSize: Int
        get() = getInt(DB_FETCH_SIZE) ?: batchSize


    val startScn: Long
        get() = getLong(START_SCN) ?: 0


    val recordPrefix: String
        get() = getString(RECORD_PREFIX)

    companion object {
        const val DB_SID = "db.name"
        const val DB_HOST = "db.hostname"
        const val DB_PORT = "db.port"
        const val DB_USERNAME = "db.user"
        const val DB_PASSWORD = "db.user.password"
        const val MONITORED_TABLES = "table.whitelist"
        const val DB_FETCH_SIZE = "db.fetch.size"
        const val START_SCN = "start.scn"
        const val TOPIC_PREFIX = "topic.prefix"
        const val RECORD_PREFIX = "record.prefix"
        const val BATCH_SIZE = "batch.size"
        fun conf(): ConfigDef {
            return ConfigDef()
                    .define(
                        DB_SID,
                        ConfigDef.Type.STRING,
                        Importance.HIGH,
                        "Database SID"
                    )
                    .define(
                        DB_HOST,
                        ConfigDef.Type.STRING,
                        Importance.HIGH,
                        "Database hostname"
                    )
                    .define(
                        DB_PORT,
                        ConfigDef.Type.INT,
                        Importance.HIGH,
                        "Database port (usually 1521)"
                    )
                    .define(
                        DB_USERNAME,
                        ConfigDef.Type.STRING,
                        Importance.HIGH,
                        "Database user"
                    )
                    .define(
                        DB_PASSWORD,
                        ConfigDef.Type.STRING,
                        Importance.HIGH,
                        "Database password"
                    )
                    .define(
                        MONITORED_TABLES,
                        ConfigDef.Type.STRING,
                        "",
                        Importance.HIGH,
                        "Tables that should be monitored, separated by ','. Tables have to be specified with schema. You can also just" +
                                "specify a schema to indicate that all tables within that schema should be monitored. Examples: 'MY_USER.TABLE, OTHER_SCHEMA'."
                    )
                    .define(
                        BATCH_SIZE,
                        ConfigDef.Type.INT,
                        1000,
                        Importance.HIGH,
                        "Batch size of rows that should be fetched in one batch"
                    )
                    .define(
                        DB_FETCH_SIZE,
                        ConfigDef.Type.INT,
                        null,
                        Importance.LOW,
                        "JDBC result set prefetch size. If not set, it will be defaulted to batch.size. The fetch" +
                                " should not be smaller than the batch size."
                    )
                    .define(
                        START_SCN,
                        ConfigDef.Type.LONG,
                        0L,
                        Importance.LOW,
                        "Start SCN, if set to 0 an initial intake from the tables will be performed."
                    )
                    .define(
                        RECORD_PREFIX,
                        ConfigDef.Type.STRING,
                        "",
                        Importance.HIGH,
                        "Prefix of the subject record. If you're using an Avro converter, this will be the namespace."
                    )
                    .define(
                        TOPIC_PREFIX,
                        ConfigDef.Type.STRING,
                        "",
                        Importance.MEDIUM,
                        "Prefix for the topic. Each monitored table will be written to a separate topic. If you want to change" +
                                "this behaviour, you can add a RegexRouter transform."
                    )
        }
    }
}