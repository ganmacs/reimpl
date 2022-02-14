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

class SegmentList(
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

class SegmentReader(
    private val segments: SegmentList,
) {
    private var offset = 0
    private var buffer: BufferedInputStream = BufferedInputStream(
        segments.next()?.let { FileInputStream(it.absolutePath) } ?: nullInputStream(),
        pageSize * 16
    )

    constructor(segments: List<Segment>) : this(SegmentList(segments))

    fun read(b: ByteArray, off: Int, len: Int): Result<Int> {
        val rlen = try {
            buffer.read(b, off, len)
        } catch (e: IOException) {
            return Result.failure(e)
        }

        if (len != rlen) {
            return Result.failure(InvalidRecord("the data is insufficient expected len=$len, actual len=$rlen"))
        }

        if (rlen != EOF) {
            offset += rlen
            return Result.success(rlen)
        }

        return segments.next()?.let {
            offset = 0
            buffer = BufferedInputStream(FileInputStream(it.absolutePath), pageSize * 16)
            buffer.reset()

            Result.success(0)
        } ?: Result.failure(EOFException("segment reader reached EOF"))
    }

    fun readAll(b: ByteArray, off: Int) {
        val len = buffer.available()
        if (b.size < len) throw RuntimeException("size is too short. required $len but ${b.size}")
        read(b, off, len)
    }

    fun available(): Boolean {
        println(buffer.available())
        println(buffer.available() > recordHeaderSize)
        println(segments.hasNext())
        return (buffer.available() > recordHeaderSize) || segments.hasNext()
    }


    fun close() {
        segments.forAllEach { it.close() }
    }
}

/*    override fun read(): Int {
        val b = ByteArray(1)
        val r = read(b)
        if (r == -1) {// EOF
            return -1
        }

        return b[0].toInt() // TODO
    }
     */
