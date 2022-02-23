package com.ganmacs.wal

import java.io.BufferedInputStream
import java.io.EOFException
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream.nullInputStream
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
    private val segments: SegmentList,
) {
    private var offset = 0
    private var buffer: BufferedInputStream = BufferedInputStream(
        segments.next()?.let { FileInputStream(it.absolutePath) } ?: nullInputStream(),
        pageSize * 16
    )

    constructor(segments: List<Segment>) : this(SegmentList(segments))

    fun readExact(b: ByteArray, off: Int, len: Int) {
        var r = 0
        while ((len - r) > 0) {
            val tt = read(b, off + r, len - r).getOrThrow()
            r += tt
        }
    }

    fun readAll(b: ByteArray, off: Int) {
        val len = buffer.available()
        if (b.size < len) throw error("size is too short. required $len but ${b.size}")
        read(b, off, len).getOrThrow()
    }

    private fun read(b: ByteArray, off: Int, len: Int): Result<Int> {
        val rlen = try {
            buffer.read(b, off, len)
        } catch (e: IOException) {
            return Result.failure(e)
        }

        if (rlen != EOF) {
            if (len != rlen) {
                return Result.failure(InvalidRecord("the data is insufficient expected len=$len, actual len=$rlen"))
            }

            offset += rlen
            return Result.success(rlen)
        }

        return segments.next()?.let {
            offset = 0
            buffer = BufferedInputStream(FileInputStream(it.absolutePath), pageSize * 16)

            Result.success(0)
        } ?: Result.failure(EOFException("segment reader reached EOF"))
    }

    fun available(): Boolean {
        println(buffer.available())
        println(segments.hasNext())
        return (buffer.available() > recordHeaderSize) || segments.hasNext()
    }

    fun close() {
        segments.forAllEach { it.close() }
    }
}
