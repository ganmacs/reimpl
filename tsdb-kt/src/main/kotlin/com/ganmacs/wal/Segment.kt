package com.ganmacs.wal

import com.ganmacs.glog
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.name

internal class SegmentRef(
    val name: String,
    val index: Int,
)

class Segment(
    val dir: Path,
    val index: Int,
) {
    private val inner: File = try {
        val file = File(dir.toString(), segmentFileName(index))
        if (!file.createNewFile()) {
            glog.debug("file:${file.absolutePath} already exists")
        }
        file
    } catch (e: IOException) {
        glog.error("cannot open file (${File(dir.toString(), segmentFileName(index)).absoluteFile} : $e")
        throw e
    }

    private val outputStream = FileOutputStream(inner)

    fun length(): Int = inner.length().toInt() // TODO: check

    fun write(b: ByteArray, off: Int, len: Int) {
        outputStream.write(b, off, len)
    }

    fun fsync() {
        outputStream.fd.sync()
    }

    fun close() {
        outputStream.close()
    }
}

/*
fun segmentIndexRange(dir: Path): Pair<Int?, Int?> {
    val refs = listSegment(dir)
    return refs.getOrNull(0)?.index to refs.getOrNull(refs.size - 1)?.index
}
*/

fun getNextSegmentIndex(dir: Path): Int = listSegments(dir).getOrNull(0)?.let { it.index + 1 } ?: 0

private fun segmentFileName(index: Int): String = String.format("%08d", index)

internal fun listSegments(dir: Path): List<SegmentRef> {
    val segRefs: MutableList<SegmentRef> = mutableListOf()

    for (file in Files.list(dir)) {
        val name = file.fileName.name
        val index = try {
            Integer.parseInt(name)
        } catch (e: NumberFormatException) {
            glog.warn { "wal file name: $name is invalid. it must be number" }
            continue
        }
        segRefs.add(SegmentRef(name = name, index = index))
    }

    segRefs.sortBy(SegmentRef::index)
    val b = segRefs.getOrNull(0)?.index ?: 0
    for ((idx, ref) in segRefs.withIndex()) {
        if ((ref.index - b) != idx) {
            throw Error("name is not sequential: ${ref.name}")
        }
    }
    return segRefs
}