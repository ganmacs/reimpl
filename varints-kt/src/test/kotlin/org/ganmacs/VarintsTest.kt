package org.ganmacs

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class VarintsTest {
    @Test
    fun putVarint() {
        val buf = UByteArray(1)
        assertEquals(1, Varints.putVarint(1, buf))
        assertEquals(listOf(0x01u.toUByte()), buf.toList())

        val buf2 = UByteArray(10)
        assertEquals(1, Varints.putVarint(300, buf2))
        assertEquals(listOf(172.toUByte(), 2.toUByte()), buf2.toList().take(2))
    }

    @Test
    fun varint() {
        assertEquals(1L, Varints.varint(ubyteArrayOf(0x01u.toUByte())))
        assertEquals(300, Varints.varint(ubyteArrayOf(172.toUByte(), 2.toUByte())))
    }
}