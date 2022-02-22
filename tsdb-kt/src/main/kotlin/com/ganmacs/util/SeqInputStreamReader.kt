package com.ganmacs.util

import java.io.InputStream

class SeqInputStreamReader(
    private val inputStreams: List<InputStream>,
) : InputStream() {
    private val iter = inputStreams.iterator()
    private var cur = nextInputStream() ?: nullInputStream()

    override fun read(b: ByteArray, off: Int, len: Int): Int {
        var rlen = cur.read(b, off, len)
        while (len > rlen) {
            cur = nextInputStream() ?: return rlen
            rlen += cur.read(b, off + rlen, len - rlen)
        }

        return rlen
    }

    override fun read(): Int {
        var t = cur.read()
        while (t == -1) { // EOF
            cur = nextInputStream() ?: return -1 // End of Streams
            t = cur.read()
        }
        return t
    }

    override fun close() {
        inputStreams.forEach { it.close() }
    }

    private fun nextInputStream(): InputStream? = if (iter.hasNext()) iter.next() else null
}