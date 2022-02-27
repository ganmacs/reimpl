package com.ganmacs.util

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
        assertEquals(13, seq.available())

        off += seq.read(buf, 0, 4)
        assertEquals(buf.take(4), "abcd".toByteArray().toList())
        assertEquals(4, off)

        assertEquals(9, seq.available())
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

    @Test
    fun `when hitting EOF`() {
        val l = listOf(
            "abcdefg".toByteArray(),
            "hijklm".toByteArray()
        )

        val seq = SeqInputStreamReader(l.map { ll -> ByteArrayInputStream(ll) })
        val buf = ByteArray(20)
        var off = 0

        val firstSize = l[0].size
        off += seq.read(buf, 0, firstSize)
        assertEquals(buf.take(firstSize), l[0].toList())
        assertEquals(firstSize, off)

        val secondSize = l[1].size
        off += seq.read(buf, 0, secondSize) //hit EOF
        assertEquals(buf.take(secondSize), l[1].toList())
        assertEquals(secondSize + firstSize, off)
    }

    @Test
    fun `when hitting EOF2`() {
        val l = listOf(
            "abcdefg".toByteArray(),
            "".toByteArray(), // last is empty
        )

        val seq = SeqInputStreamReader(l.map { ll -> ByteArrayInputStream(ll) })
        val buf = ByteArray(30)

        val size = l.fold(0) { acc, bytes -> acc + bytes.size }
        assertEquals(size, seq.read(buf, 0, size + 1))
        assertEquals(-1, seq.read(buf, 0, size + 1))
    }
}