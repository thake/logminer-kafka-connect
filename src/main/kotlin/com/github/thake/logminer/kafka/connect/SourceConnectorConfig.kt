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

    val dbNameAlias: String
        get() = getString(DB_NAME_ALIAS)

    val topic: String
        get() = getString(TOPIC_CONFIG)

    val dbName: String
        get() = getString(DB_NAME_CONFIG)

    val dbHostName: String
        get() = getString(DB_HOST_NAME_CONFIG)

    val dbPort: Int
        get() = getInt(DB_PORT_CONFIG)

    val dbUser: String
        get() = getString(DB_USER_CONFIG)

    val dbUserPassword: String
        get() = getString(DB_USER_PASSWORD_CONFIG)


    val whitelistedTables: List<String>
        get() = getString(TABLE_WHITELIST).split(",").map { it.trim() }

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
        const val DB_NAME_ALIAS = "db.name.alias"
        const val TOPIC_CONFIG = "topic"
        const val DB_NAME_CONFIG = "db.name"
        const val DB_HOST_NAME_CONFIG = "db.hostname"
        const val DB_PORT_CONFIG = "db.port"
        const val DB_USER_CONFIG = "db.user"
        const val DB_USER_PASSWORD_CONFIG = "db.user.password"
        const val TABLE_WHITELIST = "table.whitelist"
        const val DB_FETCH_SIZE = "db.fetch.size"
        const val START_SCN = "start.scn"
        const val RECORD_PREFIX = "record.prefix"
        const val BATCH_SIZE = "batch.size"
        fun conf(): ConfigDef {
            return ConfigDef()
                    .define(
                        DB_NAME_ALIAS,
                        ConfigDef.Type.STRING,
                        Importance.HIGH,
                        "Db Name Alias"
                    )
                    .define(
                        TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        Importance.HIGH,
                        "Topic"
                    )
                    .define(
                        DB_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        Importance.HIGH,
                        "Db Name"
                    )
                    .define(
                        DB_HOST_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        Importance.HIGH,
                        "Db HostName"
                    )
                    .define(
                        DB_PORT_CONFIG,
                        ConfigDef.Type.INT,
                        Importance.HIGH,
                        "Db Port"
                    )
                    .define(
                        DB_USER_CONFIG,
                        ConfigDef.Type.STRING,
                        Importance.HIGH,
                        "Db User"
                    )
                    .define(
                        DB_USER_PASSWORD_CONFIG,
                        ConfigDef.Type.STRING,
                        Importance.HIGH,
                        "Db User Password"
                    )
                    .define(
                        TABLE_WHITELIST,
                        ConfigDef.Type.STRING,
                        "",
                        Importance.HIGH,
                        "Tables will be mined"
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
                        "JDBC result set prefetch size. If not set, it will be defaulted to batch.size"
                    )
                    .define(
                        START_SCN,
                        ConfigDef.Type.LONG,
                        Importance.LOW,
                        "Start SCN"
                    )
                    .define(
                        RECORD_PREFIX,
                        ConfigDef.Type.STRING,
                        "",
                        Importance.HIGH,
                        "Prefix of the subject record"
                    )
        }
    }
}