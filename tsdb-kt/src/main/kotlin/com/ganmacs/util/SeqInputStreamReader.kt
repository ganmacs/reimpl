package com.ganmacs.util

import com.ganmacs.wal.EOF
import java.io.InputStream

internal class SeqInputStreamReader(
    private val inputStreams: List<InputStream>,
) : InputStream() {
    private val iter = inputStreams.iterator()
    private var inputStream = iter.nextOrNull() ?: nullInputStream()

    // expect that available() doesn't change while reading
    private var totalAvailable = inputStreams.fold(0) { acc, e -> acc + e.available() }

    override fun read(b: ByteArray, off: Int, len: Int): Int {
        var rlen = read0(b, off, len)
        if (rlen == EOF) return EOF

        while (len > rlen) {
            inputStream = iter.nextOrNull() ?: break
            val r = read0(b, off + rlen, len - rlen)
            if (r == EOF) { // when the last inputStream in iter is empty
                break
            }
            rlen += r
        }

        totalAvailable -= rlen
        return rlen
    }

    override fun read(): Int {
        var t = inputStream.read()
        while (t == EOF) {
            inputStream = iter.nextOrNull() ?: return EOF
            t = inputStream.read()
        }

        totalAvailable--
        return t
    }

    private fun read0(b: ByteArray, off: Int, len: Int): Int {
        var r = inputStream.read(b, off, len)
        while (r == EOF) {
            inputStream = iter.nextOrNull() ?: return EOF
            r = inputStream.read(b, off, len)
        }
        return r
    }

    override fun available(): Int = totalAvailable

    override fun close() = inputStreams.forEach { it.close() }
}

private fun <T> Iterator<T>.nextOrNull(): T? = if (hasNext()) next() else null