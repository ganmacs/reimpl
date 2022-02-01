package com.ganmacs.wal

import java.nio.ByteBuffer
import java.util.zip.CRC32

private fun ByteBuffer.putU8(b: UByte): ByteBuffer = this.put(b.toByte())
private fun ByteBuffer.putU16(value: UShort): ByteBuffer = this.putShort(value.toShort())
private fun ByteBuffer.putU32(value: UInt): ByteBuffer = this.putInt(value.toInt())

internal class Page {
    var allocated = 0
    var flushed = 0
    val buf: ByteBuffer = ByteBuffer.allocate(pageSize) // FIXME: byte can represent -127 ~ 128

    fun availableSpace(): Int = (pageSize - allocated) - recordHeaderSize

    fun bufferedDataSize(): Int = allocated - flushed

    fun clearSetup() {
        flushed = pageSize
    }

    fun full(): Boolean = availableSpace() <= 0

    fun appendRecord(type: WalType, data: ByteArray, len: Int, offset: Int): Int {
        buf.position(allocated) // to last

        buf.putU8(type.v.toUByte())
        buf.putU16(len.toUShort())
        buf.put(data, 0, len)
        CRC32().also {
            it.update(data, offset, len)
            buf.putU32(it.value.toUInt())
        }

        allocated += len + recordHeaderSize
        return len
    }

    fun clear() {
        flushed = 0
        allocated = 0
        buf.clear()
    }
}