package com.ganmacs.wal

import java.io.BufferedInputStream
import java.io.EOFException
import java.nio.file.Path

internal data class SegmentRange(
    val dir: Path,
    val from: Int,
    val to: Int,
)

class InvalidRecord(override val message: String?) : RuntimeException(message)

typealias SegmentBufReader = BufferedInputStream

internal const val EOF: Int = -1

internal fun SegmentBufReader(segments: List<Segment>): SegmentBufReader = BufferedInputStream(
    SeqInputStreamReader(segments.map { it.readOnly() }),
    pageSize * 16
)

internal fun SegmentBufReader.readExact(b: ByteArray, off: Int, len: Int): Int =
    when (val rlen = this.read(b, off, len)) {
        len -> rlen
        EOF -> throw EOFException("Segment Buf Reader reached EOF")
        else -> throw InvalidRecord("invalid size: expected $len, got $rlen")
    }
