package com.ganmacs.wal

import com.ganmacs.glog
import com.google.common.io.ByteArrayDataOutput
import com.google.common.io.ByteStreams
import java.io.EOFException
import java.nio.ByteBuffer
import java.util.zip.CRC32

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
    private var ret: ByteArray? = null
    private var total: Int = 0

    override fun hasNext(): Boolean {
        return try {
            ret = ret ?: innerNext()
            true
        } catch (e: EOFException) {
            glog.debug(e.toString())
            false
        }
    }

    override fun next(): ByteArray {
        if (!hasNext()) {
            throw error("already EOF")
        }

        val record = ret!!
        ret = null
        return record
    }

    private fun innerNext(): ByteArray {
        val out: ByteArrayDataOutput = ByteStreams.newDataOutput()
        while (true) {
            buffer.clear()

            total += reader.readExact(buffer.array(), 0, recordHeaderSize)

            val walType = buffer.readWalType()
            if (walType == WalType.PageTerm) {
                // wal writes page per pageSize, so consume 1 page here
                val len = pageSize - (total % pageSize)

                // consume remaining padding 0
                total += reader.readExact(buffer.array(), 0, len)
                if (buffer.array().none { it == 0.toByte() }) {
                    throw error("padding includes 0. something invalid")
                }
                continue
            }

            val length = buffer.readLength()
            val checksum = buffer.readChecksum()
            total += reader.readExact(buffer.array(), recordHeaderSize, length)

            val crc = CRC32()
            crc.update(buffer.array(), recordHeaderSize, length)
            if (crc.value.toUInt() != checksum) {
                throw error("checksum is invalid")
            }

            out.write(buffer.array(), recordHeaderSize, length)
            if (walType == WalType.Full || walType == WalType.Last) {
                return out.toByteArray()
            }
        }
    }
}
