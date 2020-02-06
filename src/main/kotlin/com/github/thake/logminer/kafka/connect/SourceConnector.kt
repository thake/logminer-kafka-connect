package com.github.thake.logminer.kafka.connect

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector
import java.util.*

class SourceConnector : SourceConnector() {
    private lateinit var config: SourceConnectorConfig
    override fun version(): String {
        return "1.0"
    }

    override fun start(map: Map<String, String>) {
        config = SourceConnectorConfig(map)
        val dbName: String = config.dbName
        if (dbName == "") {
            throw ConnectException("Missing Db Name property")
        }
        if (config.whitelistedTables.isEmpty()) {
            throw ConnectException("Could not find schema or table entry for connector to capture")
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