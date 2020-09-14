package com.github.thake.logminer.kafka.connect.logminer


import com.github.thake.logminer.kafka.connect.*
import mu.KotlinLogging
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp


private val logger = KotlinLogging.logger {}



/**
 * A specific runtime offset for the fetcher.
 */
data class FetcherOffset(
    val lastScn: Long,
    val lastRowId: String?,
    val commitScn: Long,
    val transactionCompleted: Boolean
) {
    constructor(lastRow: LogminerRowIdentifier, commitScn: Long, transactionCompleted: Boolean) : this(
        lastRow.scn,
        lastRow.rowId,
        commitScn,
        transactionCompleted
    )

    //If the transaction in commitScn hasn't been completely returned, we need
    //to include the rows of the transaction.
    val lowestCommitScn = if (transactionCompleted) commitScn + 1 else commitScn
    val lowestChangeScn = if (transactionCompleted && commitScn == lastScn) lastScn + 1 else lastScn

    fun toOffset(): OracleLogOffset {
        return OracleLogOffset
                .create(lastScn, commitScn, transactionCompleted)
    }
}

class LogminerFetcher(
    val conn: Connection,
    private val initialOffset: FetcherOffset,
    config: LogminerConfiguration
) :
    AutoCloseable {

    val logminerSession : LogminerSession = LogminerSession.initSession(conn,initialOffset,config)
    private var needToSkipToOffsetStart = initialOffset.lastScn == initialOffset.lowestChangeScn
    private val resultSet: ResultSet = logminerSession.openResultSet()
    val hasReachedEnd: Boolean
        get() = resultSet.isClosed || resultSet.isAfterLast

    fun nextRow(): LogminerRow? {
        var loadedRow: LogminerRow? = null
        var firstRun = true
        while (loadedRow == null && resultSet.next()) {
            //First check if we need to skip rows because we haven't found the initial offset
            if (this.needToSkipToOffsetStart) {
                val skip = skipNeeded(firstRun)
                firstRun = false
                if (skip) {
                    continue
                }
            }
            //Now do the real extracing
            loadedRow = extractRow()
        }
        return loadedRow
    }

    private fun skipNeeded(firstRun: Boolean): Boolean {
        val lastOpenRow = initialOffset.lastRowId ?: return false
        val lastScn = initialOffset.lastScn
        var skip = false
        val scn = resultSet.getLong(LogminerSchema.Fields.SCN)
        if (scn == lastScn) {
            val rowId = resultSet.getString(LogminerSchema.Fields.ROW_ID)
            if (rowId == lastOpenRow) {
                //We found the last read row id, we skip until we find a row that has CSF not set
                var nextRowBelongsToRowId = resultSet.getBoolean(LogminerSchema.Fields.CSF)
                while (nextRowBelongsToRowId && resultSet.next()) {
                    nextRowBelongsToRowId = resultSet.getBoolean(LogminerSchema.Fields.CSF)
                }
                logger.debug { "Skipped all rows until row with ID '$rowId'(including) in order to correctly set offset." }
                needToSkipToOffsetStart = false
            }
            //We skip this row
            skip = true
        } else if (!firstRun) {
            throw IllegalStateException("Couldn't find the rowId $lastOpenRow in the logs for the scn $scn. The offset seems to be wrong.")
        } else {
            logger
                    .warn { "Logminer result does not start with expected SCN. The archivelog containing the SCN seems to be deleted. The collected data has a gap between ${initialOffset.lastScn}  (expected) and $scn (actual lowest SCN in logs). All changes happening between those SCNs have not been processed and may be lost." }
            needToSkipToOffsetStart = false
        }

        return skip
    }

    private fun extractRow(): LogminerRow? {
        val operationStr = resultSet.getString(LogminerSchema.Fields.OPERATION)
        val xid = resultSet.getString(LogminerSchema.Fields.XID)
        val rowIdentifier = LogminerRowIdentifier(
            resultSet.getLong(LogminerSchema.Fields.SCN),
            resultSet.getString(LogminerSchema.Fields.ROW_ID)
        )
        return when (operationStr) {
            "COMMIT" -> LogminerRow.Commit(rowIdentifier, xid)
            "ROLLBACK" -> LogminerRow.Rollback(rowIdentifier, xid)
            else -> {
                extractChange(operationStr, rowIdentifier, xid)
            }
        }
    }

    private fun extractChange(
        operationStr: String,
        rowIdentifier: LogminerRowIdentifier,
        xid: String
    ): LogminerRow.Change? {
        val operation = Operation.valueOf(operationStr)
        val table =
            TableId(
                resultSet.getString(LogminerSchema.Fields.SEG_OWNER),
                resultSet.getString(LogminerSchema.Fields.TABLE_NAME)
            )
        val sqlRedo: String = getRedoSql(resultSet)

        val timeStamp: Timestamp = resultSet.getTimestamp(LogminerSchema.Fields.TIMESTAMP)
        val username = resultSet.getString(LogminerSchema.Fields.USERNAME)
        val status = resultSet.getInt(LogminerSchema.Fields.STATUS)
        return if (sqlRedo.contains(LogminerSchema.TEMPORARY_TABLE)) {
            null
        } else {
            LogminerRow
                    .Change(rowIdentifier, timeStamp, xid, username, table, sqlRedo, operation, status)
        }
    }


    private fun getRedoSql(resultSet: ResultSet): String {
        var sqlRedo: String = resultSet.getString(LogminerSchema.Fields.SQL_REDO)
        var contSF: Boolean = resultSet.getBoolean(LogminerSchema.Fields.CSF)
        while (contSF) {
            resultSet.next()
            sqlRedo += resultSet.getString(LogminerSchema.Fields.SQL_REDO)
            contSF = resultSet.getBoolean(LogminerSchema.Fields.CSF)
        }
        return sqlRedo
    }
    fun endLogminerSession() {
        logger.info { "Ending logminer session" }
        close()
        logminerSession.endSession()
    }
    override fun close() {
        logger.debug { "Closing fetcher of CDC records." }
        if (!resultSet.isClosed) resultSet.close()
        logminerSession.close()
    }

}

