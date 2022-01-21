package org.ganmacs

object Varints {
    fun putVarint(value: Long, buf: UByteArray): Int {
        var v = value
        var i = 0
        while (v >= 0x80) {
            buf[i] = v.toUByte() or 0x80u   // add most significant bit (MSB)
            v = v shr 7
            i++
        }

        buf[i] = v.toUByte()
        return i + 1
    }

    fun varint(buf: UByteArray): Long {
        var r: Long = 0
        var i = 0
        for (b in buf) {
            if (b < 0x80u) {
                return r.or(b.toLong().shl(i))
            }
            r = r.or((b and 0x7fu).toLong().shl(i)) // drop MSB
            i += 7
        }
        return r
    }
}