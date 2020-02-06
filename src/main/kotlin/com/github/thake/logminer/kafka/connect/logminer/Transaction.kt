package com.github.thake.logminer.kafka.connect.logminer

import com.github.thake.logminer.kafka.connect.Operation
import com.github.thake.logminer.kafka.connect.SchemaDefinition
import com.github.thake.logminer.kafka.connect.SchemaService
import com.github.thake.logminer.kafka.connect.TableId
import mu.KotlinLogging
import net.openhft.chronicle.queue.ChronicleQueue
import net.openhft.chronicle.queue.ExcerptAppender
import net.openhft.chronicle.queue.ExcerptTailer
import java.nio.file.Files
import java.sql.Connection
import java.sql.Timestamp

private val logger = KotlinLogging.logger {}

class Transaction(
    private val queueFactory: ((xid: String) -> ChronicleQueue),
    conn: Connection,
    initialRecord: LogminerRow.Change,
    private val schemaService: SchemaService,
    private val maxRecordsInMemory: Int = 10
) {
    val xid: String = initialRecord.transaction
    val transactionSchemas: MutableMap<TableId, SchemaDefinition> = mutableMapOf()
    var lastReadRowId = initialRecord.rowIdentifier.rowId
    var minScn = initialRecord.rowIdentifier.scn
        private set
    var commitScn: Long? = null
        private set
    var size: Long = 0
        private set
    private var storage: TransactionStorage = TransactionStorage.Memory()

    private enum class QueueState { WRITE, READ, CLOSED }

    private var state: QueueState =
        QueueState.WRITE
    private var stillToRead: Long = 0
    val hasMoreRecords
        get() = size > 0 && (state == QueueState.WRITE || stillToRead > 0)
    private var lastTimestamp: Long = System.currentTimeMillis()

    init {
        addChange(conn, initialRecord)
    }

    fun commit(commit: LogminerRow.Commit) {
        this.state = QueueState.READ
        this.stillToRead = size
        this.commitScn = commit.rowIdentifier.scn
    }

    fun rollback() {
        this.close()
    }

    private fun ensureStorageCapacity() {
        val currentStorage = storage
        if (currentStorage is TransactionStorage.Memory && size >= maxRecordsInMemory) {
            val filesystemStorage = TransactionStorage.Filesystem(queueFactory.invoke(xid))
            //Copy already stored entries to filesystemStorage
            (0 until size).forEach {
                val change = currentStorage.readChange()
                    ?: throw IllegalStateException("Change record is missing in current storage engine. Trying to read entry #$it")
                filesystemStorage.addChange(change)
            }
            storage = filesystemStorage
        }
    }

    fun addChange(conn: Connection, record: LogminerRow.Change) {
        if (state != QueueState.WRITE) {
            throw java.lang.IllegalStateException("No new record can be added to the queue. The queue has already been read.")
        }
        lastReadRowId = record.rowIdentifier.rowId
        //First retrieve the current schema if it is not already stored
        if (!transactionSchemas.containsKey(record.table)) {
            transactionSchemas[record.table] = schemaService.getSchema(conn, record.table)
        }
        ensureStorageCapacity()
        storage.addChange(record)
        size++
        logPerformanceStatistics()
    }

    private fun logPerformanceStatistics() {
        if (size > 1000 && size % 1000 == 0L) {
            logger.debug {
                val now = System.currentTimeMillis()
                val processingDuration = now - lastTimestamp
                val entriesPerSecond = 1000.0 / (processingDuration / 1000.0)
                lastTimestamp = now
                "Processing large transaction $xid. Currently loaded $size entries. Processed ${entriesPerSecond.format(2)} records per second."
            }
        }
    }

    private fun Double.format(digits: Int) = "%.${digits}f".format(this)

    fun readRecords(maxSize: Int): List<LogminerRow.Change> {
        if (state != QueueState.READ) {
            throw java.lang.IllegalStateException("Transaction is not yet committed or already closed. Can't read in this state.")
        }

        var continueRead = true
        val loadedRecords = mutableListOf<LogminerRow.Change>()
        while (loadedRecords.size < maxSize && continueRead) {
            val loadedRecord = storage.readChange()
            if (loadedRecord == null) {
                continueRead = false
            } else {
                loadedRecords.add(loadedRecord)
                stillToRead--
            }
        }
        if (stillToRead <= 0L) {
            close()
        }

        return loadedRecords
    }

    fun close() {
        if (state != QueueState.CLOSED) {
            this.storage.cleanup()
            this.state = QueueState.CLOSED
        }
    }
}

private sealed class TransactionStorage {

    abstract fun addChange(change: LogminerRow.Change)
    abstract fun readChange(): LogminerRow.Change?
    abstract fun cleanup()
    class Memory : TransactionStorage() {
        private val backing = mutableListOf<LogminerRow.Change>()
        private var currentIterater: Iterator<LogminerRow.Change>? = null
        override fun addChange(change: LogminerRow.Change) {
            backing.add(change)
        }

        override fun readChange(): LogminerRow.Change? {
            var iterator = currentIterater
            if (iterator == null) {
                iterator = backing.iterator()
                currentIterater = iterator
            }
            return if (iterator.hasNext()) iterator.next() else null
        }

        override fun cleanup() {
            backing.clear()
        }
    }

    class Filesystem(private val queue: ChronicleQueue) : TransactionStorage() {

        private val appender: ExcerptAppender = queue.acquireAppender()
        private var trailer: ExcerptTailer? = null

        override fun addChange(change: LogminerRow.Change) {
            appender.writeDocument {
                with(it) {
                    valueOut.int64(change.rowIdentifier.scn)
                    valueOut.text(change.rowIdentifier.rowId)
                    valueOut.text(change.sqlRedo)
                    valueOut.text(change.transaction)
                    valueOut.text(change.username)
                    valueOut.text(change.operation.name)
                    valueOut.text(change.table.owner)
                    valueOut.text(change.table.table)
                    valueOut.int64(change.timestamp.time)
                }
            }
        }

        override fun readChange(): LogminerRow.Change? {
            var trailer = this.trailer
            if (trailer == null) {
                trailer = queue.createTailer()
                this.trailer = trailer
            }
            var loadedDocument: LogminerRow.Change? = null
            trailer.readDocument {
                loadedDocument =
                    with(it) {
                        val rowIdentifier = LogminerRowIdentifier(valueIn.int64(), valueIn.text()!!)
                        val sqlRedo = valueIn.text()!!
                        val transaction = valueIn.text()!!
                        val username = valueIn.text()!!
                        val operation = Operation.valueOf(valueIn.text()!!)
                        val table = TableId(
                            valueIn.text()!!,
                            valueIn.text()!!
                        )
                        val timestamp = Timestamp(valueIn.int64())
                        LogminerRow.Change(
                            rowIdentifier = rowIdentifier,
                            timestamp = timestamp,
                            sqlRedo = sqlRedo,
                            transaction = transaction,
                            table = table,
                            username = username,
                            operation = operation
                        )
                    }
            }
            return loadedDocument
        }

        override fun cleanup() {
            val queueDirToDelete = this.queue.file()
            this.queue.close()
            //Delete all files
            if (queueDirToDelete.exists()) {
                Files.walk(queueDirToDelete.toPath()).sorted(
                    Comparator.reverseOrder()
                )
                        .forEach { Files.delete(it) }
            }
        }
    }
}