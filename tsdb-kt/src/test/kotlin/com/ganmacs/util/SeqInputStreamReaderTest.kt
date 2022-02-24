package com.ganmacs.util

import com.ganmacs.wal.SeqInputStreamReader
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import kotlin.test.assertEquals

internal class SeqInputStreamReaderTest {
    @Test
    fun `multiple seq`() {
        val seq = SeqInputStreamReader(
            listOf(
                ByteArrayInputStream("abcdefg".toByteArray()),
                ByteArrayInputStream("hijklm".toByteArray())
            )
        )

        val buf = ByteArray(20)
        var off = 0
        assertEquals(7, seq.available())

        off += seq.read(buf, 0, 4)
        assertEquals(buf.take(4), "abcd".toByteArray().toList())
        assertEquals(4, off)

        assertEquals(3, seq.available())
        off += seq.read(buf, off, 4)
        assertEquals(buf.take(8), "abcdefgh".toByteArray().toList())
        assertEquals(8, off)

        assertEquals(5, seq.available())
        off += seq.read(buf, off, 4)
        assertEquals(buf.take(12), "abcdefghijkl".toByteArray().toList())
        assertEquals(12, off)

        assertEquals(1, seq.available())
        off += seq.read(buf, off, 4)
        assertEquals(buf.take(13), "abcdefghijklm".toByteArray().toList())
        assertEquals(13, off)

        assertEquals(0, seq.available())
    }
}