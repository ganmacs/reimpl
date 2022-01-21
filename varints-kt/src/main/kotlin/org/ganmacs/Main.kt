package org.ganmacs

fun UByte.toByteString(): String =
    Integer.toBinaryString(this.toInt()).padStart(8, '0')

fun main(args: Array<String>) {
    val buf = UByteArray(Long.SIZE_BYTES + 1)
    val len = Varints.putVarint(300, buf)

    for (b in buf.take(len)) {
        print(b.toByteString())
        print(" ")
    }

    println()
    println(Varints.varint(buf))
}