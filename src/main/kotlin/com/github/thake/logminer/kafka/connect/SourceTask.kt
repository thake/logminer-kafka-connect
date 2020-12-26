package com.github.thake.logminer.kafka.connect

import com.github.thake.logminer.kafka.connect.initial.SelectSource
import com.github.thake.logminer.kafka.connect.logminer.LogminerConfiguration
import com.github.thake.logminer.kafka.connect.logminer.LogminerSource
import mu.KotlinLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.apache.kafka.connect.source.SourceTaskContext
import java.sql.Connection
import java.sql.SQLException
import java.util.*

private val logger = KotlinLogging.logger {}
class NoConnectionToDatabase : RuntimeException()
sealed class TaskState
object StoppedState : TaskState()
data class StartedState(val config: SourceConnectorConfig, val context: SourceTaskContext) : TaskState() {
    private var currentConnection: Connection? = null

    val connection : Connection?
        get() {
            var connection = currentConnection
            if(connection != null && (connection.isClosed || !connection.isValid(1000))){
                connection = null
            }
            if(connection == null){
                connection = config.openConnection()
                currentConnection = connection
            }
            return connection
    }
    var offset: Offset?
    val nameService: ConnectNameService = SourceDatabaseNameService(config.dbName)
    private val schemaService: SchemaService by lazy {
        SchemaService(nameService,config.dbZoneId)
    }
    private var source: Source<out Offset?>
    private val sourcePartition = Collections.singletonMap(TaskConstants.LOG_MINER_OFFSET, config.dbName)
    private val connectSchemaFactory = ConnectSchemaFactory(nameService, isEmittingTombstones = config.isTombstonesOnDelete)

    init {
        val workingConnection = connection ?: error("No connection to database possible at startup time. Aborting.")
        fun getTablesForOwner(owner: String): List<TableId> {
            return workingConnection.metaData.getTables(null, owner, null, arrayOf("TABLE")).use {
                val result = mutableListOf<TableId>()
                while (it.next()) {
                    result.add(TableId(owner, it.getString(3)))
                }
                result
            }
        }

        fun getTablesToFetch(): List<TableId> {
            return config.logMinerSelectors.flatMap {
                when (it) {
                    is TableSelector -> Collections.singleton(TableId(it.owner, it.tableName))
                    is SchemaSelector -> getTablesForOwner(it.owner)
                }
            }
        }

        fun getInitialSource(offset: Offset?): Source<out Offset?> {
            return when (offset) {
                is OracleLogOffset -> LogminerSource(
                    LogminerConfiguration(
                        config.logMinerSelectors,
                        config.logminerDictionarySource,
                        config.batchSize,
                        config.dbFetchSize
                    ), schemaService, offset
                )
                null, is SelectOffset ->
                    SelectSource(config.batchSize, getTablesToFetch(), schemaService, offset as? SelectOffset)
            }
        }

        fun createOffsetFromConfig(): Offset? {
            return if (config.startScn > 0) {
                OracleLogOffset.create(config.startScn, config.startScn, false)
            } else {
                null
            }
        }

        val offsetMap = context.offsetStorageReader()
                .offset(
                    sourcePartition
                ) ?: Collections.emptyMap()
        offset = Offset.create(offsetMap) ?: createOffsetFromConfig()
        source = getInitialSource(offset)
    }



    private fun createLogminerSource(): LogminerSource {
        val selectSource = source as? SelectSource
        return LogminerSource(
            LogminerConfiguration(
                config.logMinerSelectors,
                config.logminerDictionarySource,
                config.batchSize,
                config.dbFetchSize
            ),
            schemaService,
            selectSource?.getOffset()?.toOracleLogOffset() ?: OracleLogOffset.create(config.startScn, config.startScn, false)
        )
    }


    fun poll(): List<SourceRecord> {
        logger.debug { "Polling database for new changes ..." }
        fun doPoll(): List<PollResult> {
            val workingConnection = connection ?: throw NoConnectionToDatabase()
            source.maybeStartQuery(workingConnection)
            val result = source.poll()
            //Advance the offset and source
            offset = source.getOffset()
            return result
        }

        var result = doPoll()
        if (source is SelectSource && result.isEmpty()) {
            val logminerSource = createLogminerSource()
            logger
                    .info { "Initial import succeeded. Starting to read the archivelog from scn ${logminerSource.getOffset().commitScn}" }
            source = logminerSource
            result = doPoll()
        }
        //Convert the records to SourceRecords
        return result.flatMap {
            try {
                connectSchemaFactory.convertToSourceRecords(it, sourcePartition)
            } catch (e: DataException) {
                logger
                        .warn(e) { "Couldn't convert record $it to schema. This most probably indicates that supplemental logging is not activated for all columns. This record will be skipped." }
                emptyList()
            }
        }.also {
            if (it.isEmpty()) {
                logger
                        .debug { "No new changes found. Waiting ${config.pollInterval.toMillis()} ms until next poll attempt." }
                Thread.sleep(config.pollInterval.toMillis())
            } else {
                logger.info { "Found ${it.size} new changes. Submitting them to kafka." }
            }
        }
    }

    fun stop() {
        logger.info { "Kafka connect oracle task will be stopped" }
        this.source.close()
        this.connection?.close()
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
            logger.info { "Oracle Kafka Connector is starting" }
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
        return try {
            val currState = state
            if (currState is StartedState) currState.poll() else throw IllegalStateException("Task has not been started")
        } catch (e: SQLException) {
            logger.info(e) { "SQLException thrown. This is most probably due to an error while stopping." }
            Collections.emptyList()
        } catch (e : NoConnectionToDatabase){
            logger.info(e) {"Currently no connection to the database can be established. Returning 0 records to kafka."}
            Collections.emptyList()
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