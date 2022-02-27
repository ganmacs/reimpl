package com.ganmacs.wal

import org.slf4j.Logger
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.deleteExisting
import kotlin.io.path.deleteIfExists
import kotlin.math.min

internal const val pageSize = 32 * 1024                   // 32KB
internal const val recordHeaderSize = 7                   // type(1 bytes) + length(2 bytes) + CRC32(4 bytes)
internal const val defaultSegmentSize = 128 * 1024 * 1024 // 128 MB

internal enum class WalType(val v: Byte) {
    PageTerm(0),
    Full(1),
    First(2),
    Last(3),
    Middle(4);

    companion object {
        fun fromInt(b: Int): WalType = when (b) {
            0 -> PageTerm
            1 -> Full
            2 -> First
            3 -> Last
            4 -> Middle
            else -> throw RuntimeException("invalid type while converting wal type $b")
        }
    }
}

class Wal(
    private val logger: Logger,
    private val dir: Path,
    private val segmentSize: Int = defaultSegmentSize,
) {
    private val page = Page()
    private lateinit var segment: Segment
    private var donePages: Int = 0
    private var closed = false

    init {
        if (segmentSize % pageSize != 0) {
            throw Error("invalid page size")
        }

        Files.createDirectories(dir)
        val seg = Segment.create(dir, getNextSegmentIndex(dir))
        setSegment(seg)
    }

    fun close() {
        if (closed) {
            throw Error("already closed")
        }

        if (page.allocated > 0) {
            flushPage(true)
        }

        segment.fsync()
        segment.close()
        closed = true
    }

    fun truncate(i: Int) {
        for (seg in listSegments(dir)) {
            if (seg.index >= i) continue

            val p = dir.resolve(seg.name)
            p.deleteExisting()
            logger.info("deleted segment $p")
        }
    }

    fun log(bufs: List<ByteArray>) {
        for ((i, buf) in bufs.withIndex()) {
            log(buf, i == bufs.size - 1)
        }
    }

    // Do not perform fsync per log.
    // https://github.com/prometheus/prometheus/issues/5869
    private fun log(buf: ByteArray, final: Boolean) {
        // TODO: get lock
        val remaining = page.availableSpace() + // free space in page
                (pageSize - recordHeaderSize) * (remainingPagesInCurrentSegment() - 1) // free space in active segment (-1 is for using by current page)

        logger.debug(
            "buf.size: ${buf.size}, remaining: $remaining, page available: ${page.availableSpace()}," +
                    "current segment remaining: ${remainingPagesInCurrentSegment()}" +
                    ", donePage: $donePages"
        )

        if (remaining < (buf.size + recordHeaderSize)) {
            createNextSegment()
        }

        var idx = 0
        var offset = 0
        while (offset < buf.size) {
            val bsize = buf.size - offset
            val availablePageSpace = page.availableSpace() - recordHeaderSize
            val len = min(bsize, availablePageSpace)
            val type = if (availablePageSpace > bsize && idx == 0) {
                WalType.Full
            } else if (availablePageSpace > bsize) {
                WalType.Last
            } else if (idx == 0) {
                WalType.First
            } else {
                WalType.Middle
            }

            logger.debug("append len=$len, offset=$offset, type=$type")
            offset += page.appendRecord(type, data = buf, offset = offset, len = len)

            if (page.full()) {
                logger.debug("page is full")
                flushPage(true)
            }
            idx++
        }

        if (final && page.allocated > 0) {
            flushPage(false)
        }
    }

    private fun createNextSegment() {
        // for flush all data in current page
        if (page.allocated > 0) {
            flushPage(true)
        }

        val prev = segment
        val next = Segment.create(dir, segment.index + 1)
        setSegment(next)

        logger.debug("Created new segment. old=${segment.index}, new=${segment.index + 1}")
        // blocking. may be better to be executed on another thread.
        prev.fsync()
        prev.close()
    }

    private fun flushPage(clear: Boolean = false) {
        val clearFlag = clear || page.full()
        if (clearFlag) {
            page.fillData() // this is need to donePages. `len / pageSize` is necessary
        }

        val len = page.bufferedDataSize()
        logger.debug("page is flushing. segment=${segment.index} size=$len, flushed=${page.flushed}, allocated=${page.allocated}")
        segment.write(this.page.buf.array(), page.flushed, len)
        page.flushed += len

        if (clearFlag) {
            donePages++
            page.clear()
        }
    }

    private fun remainingPagesInCurrentSegment(): Int = pagesPerSegment() - donePages

    private fun pagesPerSegment(): Int = segmentSize / pageSize

    private fun setSegment(segment: Segment) {
        this.segment = segment

        val len = segment.length()
        this.donePages = len / pageSize
    }
}
