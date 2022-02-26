package com.ganmacs.wal

import com.ganmacs.util.SeqInputStreamReader
import java.io.BufferedInputStream
import java.nio.file.Path

internal data class SegmentRange(
    val dir: Path,
    val from: Int,
    val to: Int,
)

internal typealias SegmentBufReader = BufferedInputStream

internal fun SegmentBufReader(segments: List<Segment>): SegmentBufReader =
    BufferedInputStream(
        SeqInputStreamReader(segments.map { it.forRead() }),
        pageSize * 16
    )