package com.github.thake.logminer.kafka.connect

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector
import java.util.*

class LogminerSourceConnector : SourceConnector() {
    private lateinit var config: SourceConnectorConfig

    companion object {
        const val version = "1.0"
        const val name = "logminer-kafka-connect"

    }

    override fun version(): String {
        return LogminerSourceConnector.version
    }

    override fun start(map: Map<String, String>) {
        config = SourceConnectorConfig(map)
        val dbName: String = config.dbName
        if (dbName == "") {
            throw ConnectException("Missing DB logical name property")
        }
        if (config.monitoredTables.isEmpty()) {
            throw ConnectException("No table or schema to be monitored specified")
        }
    }

    override fun taskClass(): Class<out Task?> {
        return SourceTask::class.java
    }

    override fun taskConfigs(i: Int): List<Map<String, String>> {
        val configs =
            ArrayList<Map<String, String>>(1)
        configs.add(config.originalsStrings())
        return configs
    }

    override fun stop() {
        //Nothing to do
    }

    override fun config(): ConfigDef {
        return SourceConnectorConfig.conf()
    }


}