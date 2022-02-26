package com.ganmacs.wal

import com.ganmacs.glog
import com.google.common.io.ByteArrayDataOutput
import com.google.common.io.ByteStreams
import java.io.EOFException
import java.nio.ByteBuffer

private fun ByteBuffer.readU8(): UByte = this.get().toUByte()
private fun ByteBuffer.readU16(): UShort = this.short.toUShort()
private fun ByteBuffer.readU32(): UInt = this.int.toUInt()

internal fun ByteBuffer.readWalType(off: Int = 0): WalType {
    this.position(0 + off)
    return WalType.fromInt(readU8().toInt())
}

internal fun ByteBuffer.readLength(off: Int = 0): Int {
    this.position(1 + off) // skip wal type
    return readU16().toInt()
}

internal fun ByteBuffer.readChecksum(off: Int = 0): UInt {
    this.position(3 + off) // skip wal type(1) + length(2)
    return readU32()
}

internal class WalReader(
    private val reader: SegmentBufReader,
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
        if (!hasNext()) throw NoSuchElementException()

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

            val actualChecksum = crc32(buffer.array(), recordHeaderSize, length)
            if (actualChecksum != checksum) {
                throw error("checksum is invalid, expected: $checksum, actual: $actualChecksum")
            }

            out.write(buffer.array(), recordHeaderSize, length)
            if (walType == WalType.Full || walType == WalType.Last) {
                return out.toByteArray()
            }
        }
    }
}

