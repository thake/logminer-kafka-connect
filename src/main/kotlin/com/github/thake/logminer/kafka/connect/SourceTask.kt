package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.logminer.LogminerConfiguration
import com.github.thake.logminer.kafka.connect.logminer.LogminerSource
import mu.KotlinLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.apache.kafka.connect.source.SourceTaskContext
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.*

private val logger = KotlinLogging.logger {}

sealed class TaskState
object StoppedState : TaskState()
data class StartedState(val config: SourceConnectorConfig, val context: SourceTaskContext) : TaskState() {
    val connection: Connection by lazy {
        with(config) {
            val dbUri = "${dbHostName}:${dbPort}/${dbName}"
            DriverManager.getConnection(
                "jdbc:oracle:thin:@$dbUri",
                dbUser, dbUserPassword
            ).also {
                logger.info { "Connected to database at $dbUri" }
            }
        }
    }
    var offset: Offset
    private val schemaService: SchemaService by lazy {
        SchemaService(config.recordPrefix)
    }
    private var source: Source
    private val sourcePartition = Collections.singletonMap(TaskConstants.LOG_MINER_OFFSET, config.dbName)

    init {
        val offsetMap = context.offsetStorageReader()
                .offset(
                    sourcePartition
                ) ?: Collections.emptyMap()
        offset = Offset.create(offsetMap) ?: OracleLogOffset.create(config.startScn, config.startScn, false)
        source = getNextSource()
    }

    private val connectSchemaFactory = ConnectSchemaFactory(config.recordPrefix)


    private fun getNextSource(): Source {
        return offset.let { offset ->
            when (offset) {
                is OracleLogOffset -> if (source is LogminerSource) {
                    source
                } else {
                    source.close()
                    LogminerSource(
                        LogminerConfiguration(
                            config.logMinerSelectors,
                            config.batchSize,
                            config.dbFetchSize
                        ), schemaService, offset
                    )
                }
                is SelectOffset -> source
            }
        }
    }

    private fun determineTopic(record: CdcRecord) =
        with(config) {
            when (topic) {
                "" -> "$dbNameAlias.${record.table.fullName}"
                else -> topic
            }
        }

    fun poll(): List<SourceRecord> {
        logger.debug { "Polling database for new changes ..." }
        source.maybeStartQuery(connection)
        val result = source.poll()
        //Advance the offset and source
        offset = source.getOffset()
        source = getNextSource()
        //Convert the records to SourceRecords
        return result.mapNotNull {
            try {
                connectSchemaFactory.convertToSourceRecord(it, sourcePartition, determineTopic(it.cdcRecord))
            } catch (e: DataException) {
                logger
                        .warn(e) { "Couldn't convert record $it to schema. This most probably indicates that supplemental logging is not activated for all columns. This record will be skipped." }
                null
            }
        }.also {
            if (it.isEmpty()) {
                logger.debug { "No new changes found." }
            } else {
                logger.info { "Found ${it.size} new changes. Submitting them to kafka." }
            }
        }
    }

    fun stop() {
        logger.info { "Kafka connect oracle task will be stopped" }
        this.source.close()
        this.connection.close()
    }

}

object TaskConstants {
    const val LOG_MINER_OFFSET = "logminer"
}

/**
 * @author Thorsten Hake (mail@thorsten-hake.com)
 */
class SourceTask : SourceTask() {
    private var state: TaskState = StoppedState

    override fun version() = "1.0"


    override fun start(map: Map<String, String>) {
        state = StartedState(SourceConnectorConfig(map), context).apply {
            logger.info { "Oracle Kafka Connector is starting on ${config.dbNameAlias}" }
            try {
                logger.debug { "Starting LogMiner Session" }
                this.connection
                logger.debug { "Logminer started successfully" }
            } catch (e: SQLException) {
                throw ConnectException("Error at database tier, Please check : $e")
            }
        }
    }

    @Throws(InterruptedException::class)
    override fun poll(): List<SourceRecord> {
        try {
            val currState = state
            return if (currState is StartedState) currState.poll() else throw IllegalStateException("Task has not been started")
        } catch (e: SQLException) {
            logger.debug(e) { "SQLException thrown. This is most probably due to an error while stopping." }
            return Collections.emptyList()
        }
    }


    override fun stop() {
        logger.info { "Stop called for logminer" }
        (state as? StartedState)?.let {
            it.stop()
            logger.info { "Stopped logminer" }
            state = StoppedState
        }
    }
}