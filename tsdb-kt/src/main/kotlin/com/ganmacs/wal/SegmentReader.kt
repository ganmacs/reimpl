package com.ganmacs.wal

import SegmentBufReader
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.nio.file.Path

data class SegmentRange(
    val dir: Path,
    val from: Int,
    val to: Int,
)

const val EOF: Int = -1

internal class SegmentList(
    private val segments: List<Segment>,
) {
    private var cur: Int = -1

    fun next(): Segment? {
        return if (hasNext()) {
            cur++
            segments[cur]
        } else {
            null
        }
    }

    fun hasNext(): Boolean = segments.size > cur + 1

    fun forAllEach(action: (Segment) -> Unit) = segments.forEach(action)
}

class InvalidRecord(override val message: String?) : RuntimeException(message)

internal class SegmentReader(
    private val segments: InputStream,
) {

    constructor(segments: List<Segment>) : this(SegmentBufReader(segments))

    fun readExact(b: ByteArray, off: Int, len: Int): Int {
        var r = 0
        while ((len - r) > 0) {
            val tt = read(b, off + r, len - r).getOrThrow()
            r += tt
        }

        return r
    }

    fun readFull(b: ByteArray, off: Int): Int {
        return read(b, off, b.size).getOrThrow()
    }

    private fun read(b: ByteArray, off: Int, len: Int): Result<Int> {
        val rlen = try {
            segments.read(b, off, len)
        } catch (e: IOException) {
            return Result.failure(e)
        }

        if (rlen != EOF) {
            if (len != rlen) {
                return Result.failure(InvalidRecord("the data is insufficient expected len=$len, actual len=$rlen"))
            }

            return Result.success(rlen)
        } else {
            return Result.failure(EOFException("segment reader reached EOF"))
        }

    }

    fun available(): Boolean = segments.available() > recordHeaderSize


    fun close() = segments.close()
}
