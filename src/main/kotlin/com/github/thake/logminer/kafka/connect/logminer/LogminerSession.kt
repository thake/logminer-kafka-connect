package com.github.thake.logminer.kafka.connect.logminer

import com.github.thake.logminer.kafka.connect.LogminerDictionarySource
import com.github.thake.logminer.kafka.connect.SchemaSelector
import com.github.thake.logminer.kafka.connect.TableSelector
import com.github.thake.logminer.kafka.connect.logminer.LogminerSchema.END_LOGMINER_SESSION_QUERY
import mu.KotlinLogging
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

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
        const val STATUS = "STATUS"
    }

    const val TEMPORARY_TABLE = "temporary tables"
    const val NULL_VALUE = "NULL"

    const val QUERY_MONITORED_LOGS = """
        select filename as name, low_scn, next_scn from V${'$'}LOGMNR_LOGS
    """
    const val START_LOGMINER_SESSION_REDO_LOG_DICTIONARY_QUERY = """
        declare
            start_scn NUMBER := ?;
            catalog_exists NUMBER := 0;
            type logfile_name_table is table of NUMBER INDEX BY V${'$'}LOGFILE.member%TYPE;
            v_logfiles logfile_name_table;
            i V${'$'}LOGFILE.member%TYPE;
            start_scn_in_logfiles NUMBER := 0;
        begin
            --First check if a catalog exists in log files. Add all logfiles afterwards so that DDL tracking works.
            for l_cat_log_file in (
                select name,FIRST_CHANGE# from v${'$'}archived_log where NAME IS NOT NULL AND FIRST_CHANGE# >= (select max(FIRST_CHANGE#) from V${'$'}ARCHIVED_LOG where DICTIONARY_BEGIN = 'YES')
                )
            loop
                v_logfiles(l_cat_log_file.NAME) := l_cat_log_file.FIRST_CHANGE#;
                catalog_exists := 1;
            end loop;
        
            if(catalog_exists = 0) then
                -- Create a catalog
                DBMS_OUTPUT.PUT_LINE('Found no dictionary in redo logs. Building dictionary for logminer stored in redo logs.');
                DBMS_LOGMNR_D.BUILD(OPTIONS=> DBMS_LOGMNR_D.STORE_IN_REDO_LOGS);
            else
                DBMS_OUTPUT.PUT_LINE('Found a dictionary in redo logs. Using it as a reference.');
            end if;
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
                   v_logfiles(l_log_rec.name) := l_log_rec.FIRST_CHANGE#;
                end loop;
            i := v_logfiles.FIRST;
            WHILE i IS NOT NULL LOOP
                if i = v_logfiles.FIRST then
                    DBMS_OUTPUT.PUT_LINE('Restarting logminer with logfile '||i);
                    DBMS_LOGMNR.ADD_LOGFILE(i,DBMS_LOGMNR.NEW);
                else
                    DBMS_OUTPUT.PUT_LINE('Adding logfile: '||i);
                    DBMS_LOGMNR.ADD_LOGFILE(i);
                end if;
                if(v_logfiles(i) <= start_scn) then
                    start_scn_in_logfiles := 1;
                end if;
                i := v_logfiles.NEXT(i);  -- Get next element of array
            END LOOP;
            if(start_scn_in_logfiles = 0) then
                DBMS_OUTPUT.PUT_LINE('Start scn has not been found in available log files. Setting start scn to earliest available scn');
                start_scn := 0;
            end if;
            DBMS_LOGMNR.START_LOGMNR(startScn => start_scn,
                    OPTIONS => DBMS_LOGMNR.SKIP_CORRUPTION + DBMS_LOGMNR.NO_SQL_DELIMITER + DBMS_LOGMNR.NO_ROWID_IN_STMT +
                               DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING +
                               dbms_logmnr.STRING_LITERALS_IN_STMT);
        end;
    """
    const val START_OR_UPDATE_LOGMINER_SESSION_ONLINE_QUERY = """
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
    const val UPDATE_MINER_QUERY = """
        declare
            start_scn NUMBER := ?;
            file_added NUMBER := 0;
        begin
            for l_log_rec IN (select name,SEQUENCE#,FIRST_CHANGE#,NEXT_CHANGE# from (select min(name) name, SEQUENCE#, FIRST_CHANGE#, NEXT_CHANGE#
                                                                                     from (
                                                                                              select min(member) as name, SEQUENCE#, FIRST_CHANGE#, NEXT_CHANGE#
                                                                                              from V${'$'}LOG l
                                                                                                       inner join V${'$'}LOGFILE f on l.GROUP# = f.GROUP#
                                                                                              group by SEQUENCE#,first_change#, NEXT_CHANGE#
                                                                                              union
                                                                                              select name,SEQUENCE#, FIRST_CHANGE#, NEXT_CHANGE#
                                                                                              From V${'$'}ARCHIVED_LOG
                                                                                              where name is not null)
                                                                                     where FIRST_CHANGE# >= start_scn OR start_scn < NEXT_CHANGE#
                                                                                     group by SEQUENCE#,first_change#, next_change#
                                                                                    ) needed_logs where SEQUENCE# not in (select log_ID from V${'$'}LOGMNR_LOGS) order by FIRST_CHANGE#)
            loop
                    DBMS_LOGMNR.ADD_LOGFILE(l_log_rec.name);
                    DBMS_OUTPUT.PUT_LINE('Added logfile '||l_log_rec.name||' to logminer session');
                    file_added := 1;
            end loop;
            if(file_added > 0) then
                DBMS_LOGMNR.START_LOGMNR(startScn => start_scn,
                                        OPTIONS => DBMS_LOGMNR.SKIP_CORRUPTION + DBMS_LOGMNR.NO_SQL_DELIMITER + DBMS_LOGMNR.NO_ROWID_IN_STMT +
                                                   DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING +
                                                   dbms_logmnr.STRING_LITERALS_IN_STMT);
                DBMS_OUTPUT.PUT_LINE('Restarted logminer with added logfiles loaded and start scn '||start_scn);
            else
                DBMS_OUTPUT.PUT_LINE('No new logfile has been added. Restart of logminer was not needed');
            end if;
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
                    xid,
                    status
                FROM  v${'$'}logmnr_contents  
                WHERE ROLLBACK = 0
                    AND (
                        (scn >= ? AND OPERATION_CODE IN (7,36) AND USERNAME not in ('UNKNOWNX','KMINER')) 
                            OR 
                        (scn >= ? AND OPERATION_CODE in (1,2,3) and ( 
            """
    const val QUERY_LOGMINER_END = ")))"

    const val END_LOGMINER_SESSION_QUERY = """
        begin 
            DBMS_LOGMNR.END_LOGMNR();
        end;
    """
}
data class Log(
        val name : String,
        val scnRange : LongRange
)
sealed class DictionaryStrategy{
    fun initSession(conn: Connection, offset: FetcherOffset) {
        logger.info { "Checking if a session is already running" }
        val monitoredLogs = determineMonitoredLogs(conn)
        val fromScn = offset.lowestChangeScn
        if (monitoredLogs.isEmpty() || monitoredLogs.none { it.scnRange.contains(fromScn) }) {
            logger.info { "Starting oracle logminer with scn $fromScn" }
            initSession(conn, fromScn)
        } else {
            logger.info { "Logminer session already exists. Refreshing existing session." }
            updateSession(conn, fromScn)
        }
        logger.debug {
            val nowMonitoredLogs = determineMonitoredLogs(conn)
            "Monitoring ${nowMonitoredLogs.size} Logfiles"
        }
    }
    protected abstract fun initSession(conn : Connection, fromScn: Long)
    protected abstract fun updateSession(conn : Connection, fromScn: Long)
    protected fun determineMonitoredLogs(conn : Connection): Set<Log> {
        return conn.prepareStatement(LogminerSchema.QUERY_MONITORED_LOGS).use { stmt ->
            stmt.executeQuery().use {
                val monitoredLogs = mutableSetOf<Log>()
                while (it.next()) {
                    monitoredLogs.add(Log(it.getString(1),LongRange(it.getLong(2),it.getLong(3))))
                }
                monitoredLogs
            }
        }
    }

    object RedoLogStrategy : DictionaryStrategy() {

        override fun initSession(conn : Connection, fromScn : Long){
            conn.prepareCall(LogminerSchema.START_LOGMINER_SESSION_REDO_LOG_DICTIONARY_QUERY).use {
                it.setLong(1, fromScn)
                it.execute()
            }
        }

        override fun updateSession(conn: Connection, fromScn: Long) {
            logger.info { "Updating oracle logminer session to include newest logfiles and to start from scn $fromScn" }
            conn.prepareCall(LogminerSchema.UPDATE_MINER_QUERY).use {
                it.setLong(1, fromScn)
                it.execute()
            }
        }
    }
    object OnlineLogStrategy : DictionaryStrategy(){
        override fun initSession(conn: Connection, fromScn: Long) {
            conn.prepareCall(LogminerSchema.START_OR_UPDATE_LOGMINER_SESSION_ONLINE_QUERY).use{
                it.setLong(1,fromScn)
                it.execute()
            }
        }

        override fun updateSession(conn: Connection, fromScn: Long) = initSession(conn,fromScn)


    }
}
class LogminerSession(private val conn : Connection,
                                private val offset : FetcherOffset,
                                private val config: LogminerConfiguration){

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
    private val stmt: PreparedStatement =
            conn.prepareStatement(
                    logMinerSelectSql,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY
            ).apply {
                fetchSize = config.fetchSize
                logger.debug { "Querying oracle logminer with fetch size $fetchSize" }
                setLong(1, offset.lowestCommitScn)
                setLong(2, offset.lowestChangeScn)
            }
    fun openResultSet(): ResultSet {
        logger.debug { "Select statement for oracle logminer(scn = ${offset.lastScn}): $logMinerSelectSql" }
        return stmt.executeQuery()
    }
    fun endSession(){
        close()
        conn.prepareCall(END_LOGMINER_SESSION_QUERY).use {
            it.execute()
        }
    }
    fun close(){
        if (!stmt.isClosed) stmt.close()
    }

    companion object {
        fun initSession(conn: Connection, offset: FetcherOffset, config: LogminerConfiguration) : LogminerSession{
            val strategy = when(config.logminerDictionarySource){
                LogminerDictionarySource.REDO_LOG -> DictionaryStrategy.RedoLogStrategy
                LogminerDictionarySource.ONLINE ->  DictionaryStrategy.OnlineLogStrategy
            }
            strategy.initSession(conn,offset)
            return LogminerSession(conn,offset,config)
        }

    }
}