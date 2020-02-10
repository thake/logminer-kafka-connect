package com.github.thake.logminer.kafka.connect

enum class Operation(val stringRep: String) {
    READ("r"),
    UPDATE("u"),
    INSERT("i"),
    DELETE("d")
}