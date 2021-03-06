package com.ganmacs.wal

import java.nio.ByteBuffer
import java.util.zip.CRC32

private fun ByteBuffer.putU8(b: UByte): ByteBuffer = this.put(b.toByte())
private fun ByteBuffer.putU16(value: UShort): ByteBuffer = this.putShort(value.toShort())
private fun ByteBuffer.putU32(value: UInt): ByteBuffer = this.putInt(value.toInt())

internal fun crc32(b: ByteArray, off: Int, len: Int) = CRC32().apply { update(b, off, len) }.value.toUInt()

internal class Page {
    var allocated = 0
        private set
    var flushed = 0

    var buf: ByteBuffer = ByteBuffer.allocate(pageSize) // FIXME: byte can represent -127 ~ 128

    fun availableSpace(): Int = pageSize - allocated

    fun bufferedDataSize(): Int = allocated - flushed

    fun fillData() {
        allocated = pageSize
    }

    fun full(): Boolean = availableSpace() < recordHeaderSize

    fun appendRecord(type: WalType, data: ByteArray, len: Int, offset: Int): Int {
        buf.position(allocated) // to last

        buf.putU8(type.v.toUByte())
        buf.putU16(len.toUShort())
        buf.putU32(crc32(data, offset, len))
        buf.put(data, offset, len)

        allocated += len + recordHeaderSize
        return len
    }

    fun clear() {
        flushed = 0
        allocated = 0
        buf = ByteBuffer.allocate(pageSize) // TODO: just reset, not create new bytebuffer...
    }
}