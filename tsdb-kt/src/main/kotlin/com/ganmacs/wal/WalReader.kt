package com.ganmacs.wal

import java.nio.ByteBuffer
import java.util.zip.CRC32

//private fun ByteBuffer.readU8() =
private fun ByteBuffer.readU8(): UByte = this.get().toUByte()
private fun ByteBuffer.readU16(): UShort = this.short.toUShort()
private fun ByteBuffer.readU32(): UInt = this.int.toUInt()

private fun ByteBuffer.readWalType(): WalType {
    this.position(0)
    return WalType.fromInt(readU8().toInt())
}

private fun ByteBuffer.readLength(): Int {
    this.position(1) // skip wal type
    return readU16().toInt()
}

private fun ByteBuffer.readChecksum(): UInt {
    this.position(3) // skip wal type(1) + length(2)
    return readU32()
}

internal class WalReader(
    private val reader: SegmentReader,
) : Iterator<ByteArray> {
    private val buffer = ByteBuffer.allocate(pageSize)

    override fun hasNext(): Boolean = reader.available()

    override fun next(): ByteArray {
        reader.read(buffer.array(), 0, recordHeaderSize).getOrThrow() // TODO
        while (true) {
            val walType = buffer.readWalType()
            if (walType == WalType.PageTerm) {
                // consume remaining padding 0
                reader.readAll(buffer.array(), 0)
                if (buffer.array().none { it == 0.toByte() }) {
                    throw RuntimeException("padding includes 0. something invalid")
                }
                return byteArrayOf()
            }

            val length = buffer.readLength()
            val checksum = buffer.readChecksum()
            val record = ByteArray(length)

            reader.read(record, 0, length).getOrThrow() // TODO

            val crc = CRC32()
            crc.update(record, 0, length)
            if (crc.value.toUInt() != checksum) {
                throw RuntimeException("checksum is invalid")
            }

            if (walType == WalType.Full) {
                return record
            }
        }
    }
}
