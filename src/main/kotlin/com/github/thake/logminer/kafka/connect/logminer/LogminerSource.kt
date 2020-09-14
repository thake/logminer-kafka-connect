package com.github.thake.logminer.kafka.connect.logminer


import com.github.thake.logminer.kafka.connect.*
import mu.KotlinLogging
import java.sql.Connection

private val logger = KotlinLogging.logger {}


data class LogminerConfiguration(
    val logMinerSelectors: List<LogMinerSelector>,
    val logminerDictionarySource : LogminerDictionarySource,
    val batchSize: Int = 1000,
    val fetchSize: Int = batchSize
)

class LogminerSource(
    private val config: LogminerConfiguration,
    schemaService: SchemaService,
    offset: OracleLogOffset
) : Source<OracleLogOffset> {
    private var currentState: QueryStartedState? = null
    private var lastOffset: FetcherOffset =
        FetcherOffset(offset.scn, null, offset.commitScn, offset.isTransactionComplete)
    private var transactionConsolidator: TransactionConsolidator = TransactionConsolidator(schemaService)

    override fun getOffset() = (currentState?.readLogOffset ?: lastOffset).toOffset()

    override fun maybeStartQuery(db: Connection) {
        val state = currentState

        if (state != null && state.needsRestart(db)) {
            //Now reset the current fetcher if it is not valid for the db or it has no more results to deliver.
            lastOffset = state.readLogOffset
            state.close()
            currentState = null
        }
        //Now set the state to started with a new Fetcher
        if (currentState == null) {
            val fetcher = LogminerFetcher(db, lastOffset, config)
            transactionConsolidator.conn = db
            currentState = QueryStartedState(fetcher, config.batchSize,transactionConsolidator , lastOffset)
        }
    }
    override fun poll(): List<PollResult> {
        return currentState.let {
            if (it == null) {
                throw IllegalStateException("Query has not been initialized")
            }
            it.poll()
        }
    }
    fun stopLogminer() {
        currentState?.fetcher?.endLogminerSession()
    }
    override fun close() {
        currentState?.close()
        currentState = null
        transactionConsolidator.clear()
    }

    private class QueryStartedState(
        val fetcher: LogminerFetcher,
        val batchSize: Int,
        val transactionConsolidator: TransactionConsolidator,
        var readLogOffset: FetcherOffset
    ) {
        private var forcedRestart = false
        fun needsRestart(conn : Connection) : Boolean {
            return forcedRestart || !isValidFor(conn) || fetcher.hasReachedEnd
        }

        fun isValidFor(conn: Connection) = fetcher.conn == conn

        fun poll(): List<PollResult> {
            val result = mutableListOf<PollResult>()

            logger.debug { "Fetching records. Batch size is $batchSize" }
            var continueLoop = true
            while (continueLoop) {
                result.addAll(transactionConsolidator.getOutstandingCommittedResults(batchSize))
                //Only fetch new rows from database if we haven't reached the batch size.
                continueLoop = if (result.size < batchSize) {
                    val hadMoreRows = processNextLogminerRow()
                    hadMoreRows || transactionConsolidator.hasOutstandingCommittedResults
                } else {
                    false
                }
            }

            logger.debug { "New fetcher offset: $readLogOffset" }
            return result
        }

        private fun processNextLogminerRow(): Boolean {
            var nextRow : LogminerRow = fetcher.nextRow() ?: return false
            while(nextRow is LogminerRow.Change && nextRow.status == 2){
                val change = nextRow
                logger.warn { """
                    Fetched a not readable row from logminer. This most probably indicates that a DDL statement has been executed which adds a new column with a not null default value. 
                    Oracle internally first updates all existing rows with the default value and only afterwards executes the alter statement. Thus logminer has no information of the new column when reading the update statements. 
                    
                    If this is the case, consider changing the way a not null column with default value will be added. Instead of doing everything in one command, one could separate it into the following steps:
                      1. Adding new nullable column
                      2. Adding a trigger on insert that automatically inserts the default value for the new nullable column if it is not specified.
                      3. Updating the column with the default value for all existing rows
                      4. Changing the definition of the column to be NOT NULL with the default value.
                      5. Dropping trigger on insert
                    Performing the DDL in this way would guarantee that the change log will be readable by logminer.
                    
                    Skipped not readable SQL: ${change.sqlRedo}""".trimIndent()
                }
                nextRow = fetcher.nextRow() ?: return false
            }

            when (nextRow) {
                is LogminerRow.Commit -> transactionConsolidator.commit(nextRow)
                is LogminerRow.Rollback -> transactionConsolidator.rollback(nextRow)
                is LogminerRow.Change -> transactionConsolidator.addChange(nextRow)
            }
            readLogOffset =
                FetcherOffset(
                    nextRow.rowIdentifier,
                    transactionConsolidator.lastCommitScn ?: readLogOffset.commitScn,
                    true
                )
            return true
        }


        fun close() {
            fetcher.close()
        }
    }


}
