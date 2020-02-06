package com.github.thake.logminer.kafka.connect

const val TYPE_KEY = "type"
const val CDC_TYPE = "cdc"
const val SELECT_TYPE = "select"

sealed class Offset {
    abstract val map: Map<String, Any?>

    companion object {
        fun create(map: Map<String, Any?>): Offset? = when (map[TYPE_KEY]) {
            CDC_TYPE -> OracleLogOffset(map)
            SELECT_TYPE -> SelectOffset(map)
            else -> null
        }
    }
}

class OracleLogOffset(
    override val map: Map<String, Any?>
) : Offset() {
    val scn: Long by map
    val commitScn: Long by map
    val isTransactionComplete: Boolean by map

    companion object {
        fun create(scn: Long, commitScn: Long, isTransactionComplete: Boolean) =
            OracleLogOffset(
                mapOf(
                    TYPE_KEY to CDC_TYPE,
                    "scn" to scn,
                    "commitScn" to commitScn,
                    "isTransactionComplete" to isTransactionComplete
                )
            )
    }
}

class SelectOffset(override val map: Map<String, Any?>) : Offset() {
    val table: String by map
    val scn: Long by map
    val rowId: String by map

    companion object {
        fun create(scn: Long, commitScn: Long? = null, rowId: String? = null) =
            OracleLogOffset(mapOf(TYPE_KEY to SELECT_TYPE, "scn" to scn, "commitScn" to commitScn, "rowId" to rowId))
    }
}