package com.ganmacs

import com.ganmacs.wal.Wal
import com.ganmacs.wal.pageSize
import mu.KotlinLogging
import java.nio.file.Path

val glog = KotlinLogging.logger("global tsdb")

// fun Byte.toStringByte(): String = Integer.toBinaryString(this.toUByte().toInt()).padStart(8, '0')
// fun printIntArray(buf: ByteArray) = println(buf.map { it.toStringByte() })

fun main() {
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")
    val logger = KotlinLogging.logger("tsdb")

    val series = "http_requests_total{job=\"prometheus\",group=\"canary\"}".toByteArray()

    val wal = Wal(
        logger = logger,
        dir = Path.of("./wal"),
        segmentSize = pageSize
    )

    val l = mutableListOf<String>()

    for (i in 1..1000) {
        l.add("http_requests_total{job=\"prometheus\",group=\"canary\",id=$i}")
    }

    wal.log(l.map(String::toByteArray))

    logger.info { "hello" }
}