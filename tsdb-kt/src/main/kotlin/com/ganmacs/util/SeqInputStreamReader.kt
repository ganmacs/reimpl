package com.ganmacs.wal

import java.io.InputStream

class SeqInputStreamReader(
    private val inputStreams: List<InputStream>,
) : InputStream() {
    private val iter = inputStreams.iterator()
    private var cur = iter.nextOrNull() ?: nullInputStream()

    override fun read(b: ByteArray, off: Int, len: Int): Int {
        var rlen = cur.read(b, off, len)
        while (len > rlen) {
            cur = iter.nextOrNull() ?: return rlen
            rlen += cur.read(b, off + rlen, len - rlen)
        }

        return rlen
    }

    override fun read(): Int {
        var t = cur.read()
        while (t == com.ganmacs.util.EOF) {
            cur = iter.nextOrNull() ?: return com.ganmacs.util.EOF
            t = cur.read()
        }
        return t
    }

    override fun close() = inputStreams.forEach { it.close() }
}

private fun <T> Iterator<out T>.nextOrNull(): T? = if (hasNext()) next() else null