package com.ganmacs.wal

import com.ganmacs.util.SeqInputStreamReader
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.nio.file.Path
import kotlin.io.path.Path
import kotlin.io.path.absolutePathString

internal data class SegmentRange(
    val dir: Path,
    val from: Int,
    val to: Int,
)

internal typealias SegmentBufReader = BufferedInputStream

internal fun List<SegmentRange>.toSegmentReader(): SegmentBufReader {
    val segments = this.flatMap { segmentRange ->
        listSegments(segmentRange.dir)
            .filter { segmentRef ->
                segmentRange.from >= 0 && segmentRange.to >= 0 && segmentRef.index <= segmentRange.to && segmentRef.index >= segmentRange.from
            }.map {
                val path = Path(segmentRange.dir.absolutePathString(), it.name).absolutePathString()
                FileInputStream(File(path))
            }
    }

    return BufferedInputStream(SeqInputStreamReader(segments), pageSize * 16)
}