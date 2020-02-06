package com.github.thake.logminer.kafka.connect

data class PollResult(
    val cdcRecord: CdcRecord,
    val offset: Offset
)