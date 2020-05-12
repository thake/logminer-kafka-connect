package com.github.thake.logminer.kafka.connect.logminer


import com.github.thake.logminer.kafka.connect.*
import mu.KotlinLogging
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp


private val logger = KotlinLogging.logger {}

/**
 * Describes the logminer schema and all SQL calls needed to fetch results from the logminer *
 */
object LogminerSchema {
    object Fields {
        const val SEG_OWNER = "SEG_OWNER"
        const val TABLE_NAME = "TABLE_NAME"
        const val TIMESTAMP = "TIMESTAMP"
        const val SQL_REDO = "SQL_REDO"
        const val OPERATION = "OPERATION"
        const val USERNAME = "USERNAME"
        const val XID = "xid"
        const val SCN = "SCN"
        const val ROW_ID = "ROW_ID"
        const val CSF = "CSF"
    }

    const val TEMPORARY_TABLE = "temporary tables"
    const val NULL_VALUE = "NULL"
    const val QUERY_LOGS_TO_MONITOR = """
        select min(name) name, FIRST_CHANGE#, NEXT_CHANGE#
          from (
                   select min(member) as name, FIRST_CHANGE#, NEXT_CHANGE#
                   from V${'$'}LOG l
                            inner join V${'$'}LOGFILE f on l.GROUP# = f.GROUP#
                   group by first_change#, NEXT_CHANGE#
                   union
                   select name, FIRST_CHANGE#, NEXT_CHANGE#
                   From V${'$'}ARCHIVED_LOG
                   where name is not null)
          where FIRST_CHANGE# >= ? OR ? < NEXT_CHANGE#
          group by first_change#, next_change#
            order by FIRST_CHANGE#
    """
    const val QUERY_MONITORED_LOGS = """
        select filename as name from V${'$'}LOGMNR_LOGS
    """
    const val START_OR_UPDATE_MINER_QUERY = """
        declare
            st BOOLEAN := TRUE;
            start_scn NUMBER := ?;
        begin
            for l_log_rec IN (select min(name) name, FIRST_CHANGE#, NEXT_CHANGE#
                              from (
                                       select min(member) as name, FIRST_CHANGE#, NEXT_CHANGE#
                                       from V${'$'}LOG l
                                                inner join V${'$'}LOGFILE f on l.GROUP# = f.GROUP#
                                       group by first_change#, NEXT_CHANGE#
                                       union
                                       select name, FIRST_CHANGE#, NEXT_CHANGE#
                                       From V${'$'}ARCHIVED_LOG
                                       where name is not null)
                              where FIRST_CHANGE# >= start_scn OR start_scn < NEXT_CHANGE#
                              group by first_change#, next_change#
                                order by FIRST_CHANGE#)
                loop
                    if st then
                        DBMS_LOGMNR.ADD_LOGFILE(l_log_rec.name,DBMS_LOGMNR.NEW);
                        st := FALSE;
                    else
                        DBMS_LOGMNR.ADD_LOGFILE(l_log_rec.name);
                        end if;
                end loop;
            DBMS_LOGMNR.START_LOGMNR(
                    OPTIONS => DBMS_LOGMNR.SKIP_CORRUPTION + DBMS_LOGMNR.NO_SQL_DELIMITER + DBMS_LOGMNR.NO_ROWID_IN_STMT +
                               DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG +
                               dbms_logmnr.STRING_LITERALS_IN_STMT);
        end;
    """
    const val QUERY_LOGMINER_START =
        """
                SELECT 
                    scn, 
                    commit_scn, 
                    timestamp, 
                    operation,                     
                    seg_owner, 
                    table_name, 
                    username, 
                    sql_redo, 
                    row_id, 
                    CSF,
                    xid 
                FROM  v${'$'}logmnr_contents  
                WHERE ROLLBACK = 0
                    AND (
                        (scn >= ? AND OPERATION_CODE IN (7,36) AND USERNAME not in ('UNKNOWNX','KMINER')) 
                            OR 
                        (scn >= ? AND OPERATION_CODE in (1,2,3) and ( 
            """
    const val QUERY_LOGMINER_END = ")))"
}

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
    private val config: LogminerConfiguration,
    updateLogminer: Boolean
) :
    AutoCloseable {
    private val logMinerSelectSql: String by lazy {
        config.logMinerSelectors.joinToString(
            separator = " OR ",
            prefix = LogminerSchema.QUERY_LOGMINER_START,
            postfix = LogminerSchema.QUERY_LOGMINER_END
        ) {
            when (it) {
                is TableSelector -> "(${LogminerSchema.Fields.SEG_OWNER} ='${it.owner}' and ${LogminerSchema.Fields.TABLE_NAME} = '${it.tableName}')"
                is SchemaSelector -> "(${LogminerSchema.Fields.SEG_OWNER} ='${it.owner}')"
            }
        }
    }
    private val scn = initialOffset.lastScn
    private var needToSkipToOffsetStart = initialOffset.lastScn == initialOffset.lowestChangeScn

    private val stmt: PreparedStatement =
        conn.prepareStatement(
            logMinerSelectSql,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY
        ).apply {
            fetchSize = config.fetchSize
            logger.debug { "Querying oracle logminer with fetch size $fetchSize" }
            setLong(1, initialOffset.lowestCommitScn)
            setLong(2, initialOffset.lowestChangeScn)
        }
    private val resultSet: ResultSet
    private val monitoredLogs: Set<String>
    val isLogminerOutdated: Boolean
        get() = monitoredLogs != determineLogsToMonitor()
    val hasReachedEnd: Boolean
        get() = resultSet.isClosed || resultSet.isAfterLast

    init {
        if (updateLogminer) {
            startOrUpdateLogMiner()
        }
        this.monitoredLogs = determineMonitoredLogs()
        resultSet = openResultSet()
    }

    private fun determineLogsToMonitor(): Set<String> {
        return conn.prepareStatement(LogminerSchema.QUERY_LOGS_TO_MONITOR).use { stmt ->
            stmt.setLong(1, scn)
            stmt.setLong(2, scn)
            stmt.executeQuery().use {
                val logsToMonitor = mutableSetOf<String>()
                while (it.next()) {
                    logsToMonitor.add(it.getString(1))
                }
                logsToMonitor
            }
        }
    }

    private fun determineMonitoredLogs(): Set<String> {
        return conn.prepareStatement(LogminerSchema.QUERY_MONITORED_LOGS).use { stmt ->
            stmt.executeQuery().use {
                val monitoredLogs = mutableSetOf<String>()
                while (it.next()) {
                    monitoredLogs.add(it.getString(1))
                }
                monitoredLogs
            }
        }
    }


    private fun startOrUpdateLogMiner() {
        logger.info { "Starting or updating oracle logminer with scn $scn" }
        conn.prepareCall(LogminerSchema.START_OR_UPDATE_MINER_QUERY).use {
            it.setLong(1, scn)
            it.execute()
        }
        logger.info {
            @Suppress("SqlResolve") val usedLogfiles =
                conn.prepareStatement("SELECT COUNT(*) FROM V${'$'}LOGMNR_LOGS").use { statement ->
                    statement.executeQuery().use {
                        if (it.next()) {
                            it.getInt(1)
                        } else {
                            null
                        }
                    }
                }
            "Monitoring $usedLogfiles Logfiles"
        }
    }

    private fun openResultSet(): ResultSet {
        logger.debug { "Select statement for oracle logminer(scn = $scn): $logMinerSelectSql" }
        return stmt.executeQuery()
    }


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
        return if (sqlRedo.contains(LogminerSchema.TEMPORARY_TABLE)) {
            null
        } else {
            LogminerRow
                    .Change(rowIdentifier, timeStamp, xid, username, table, sqlRedo, operation)
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

    override fun close() {
        logger.debug { "Closing fetcher of CDC records." }
        if (!resultSet.isClosed) resultSet.close()
        if (!stmt.isClosed) stmt.close()
    }

}

