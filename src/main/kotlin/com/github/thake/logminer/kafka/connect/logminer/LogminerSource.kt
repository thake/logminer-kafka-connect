package com.github.thake.logminer.kafka.connect.logminer


import com.github.thake.logminer.kafka.connect.*
import mu.KotlinLogging
import java.sql.Connection

private val logger = KotlinLogging.logger {}


data class LogminerConfiguration(
    val logMinerSelectors: List<LogMinerSelector>,
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
    private val transactionConsolidator: TransactionConsolidator =
        TransactionConsolidator(schemaService)

    override fun getOffset() = (currentState?.readLogOffset ?: lastOffset).toOffset()

    override fun maybeStartQuery(db: Connection) {
        val state = currentState
        var logMinerNeedsUpdate = true
        if (state != null) {
            //First check if the logminer needs to be updated, based on the current state.
            logMinerNeedsUpdate = state.fetcher.isLogminerOutdated || !state.isValidFor(db)
            //Now reset the current fetcher if it is not valid for the db or it has no more results to deliver.
            if (!state.isValidFor(db) || state.fetcher.hasReachedEnd) {
                lastOffset = state.readLogOffset
                state.close()
                currentState = null
            }
        }
        //Now set the state to started with a new Fetcher
        if (currentState == null) {
            val fetcher = LogminerFetcher(db, lastOffset, config, logMinerNeedsUpdate)
            currentState = QueryStartedState(fetcher, config.batchSize, transactionConsolidator, lastOffset)
        }
    }

    private class QueryStartedState(
        val fetcher: LogminerFetcher,
        val batchSize: Int,
        val transactionConsolidator: TransactionConsolidator,
        var readLogOffset: FetcherOffset
    ) {

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
            val nextRow = fetcher.nextRow() ?: return false

            when (nextRow) {
                is LogminerRow.Commit -> transactionConsolidator.commit(nextRow)
                is LogminerRow.Rollback -> transactionConsolidator.rollback(nextRow)
                is LogminerRow.Change -> transactionConsolidator.addChange(fetcher.conn, nextRow)
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

    override fun poll(): List<PollResult> {
        return currentState.let {
            if (it == null) {
                throw IllegalStateException("Query has not been initialized")
            }
            it.poll()
        }
    }

    override fun close() {
        currentState?.close()
        currentState = null
        transactionConsolidator.clear()
    }
}
