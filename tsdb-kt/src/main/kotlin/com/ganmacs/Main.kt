package com.ganmacs

import mu.KotlinLogging
import org.slf4j.Logger
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

const val pageSize = 32 * 1024         // 32KB
const val recordHeaderSize = 7 // type(1 bytes) + length(2 bytes) + CRC32 (4 bytes)

val glog = KotlinLogging.logger("global tsdb")

data class SegmentRef(
    val name: String,
    val index: Int,
)

class Segment(dir: Path, index: Int) {
    val inner = try {
        File(dir.toString(), fileName(index)).createNewFile()
    } catch (e: IOException) {
        glog.error("cannot open file(${File(dir.toString(), fileName(index)).absoluteFile} : $e")
        throw e
    }

    companion object {
        fun fileName(index: Int): String = String.format("%08d", index);

        fun list(dir: Path): List<SegmentRef> {
            val segRefs: MutableList<SegmentRef> = mutableListOf()

            for (file in Files.list(dir)) {
                val name = file.fileName.toString()
                val index = try {
                    Integer.parseInt(name)
                } catch (e: NumberFormatException) {
                    glog.warn { "wal file name: $name is invalid. it must be number" }
                    continue
                }
                segRefs.add(SegmentRef(name = name, index = index))
            }

            segRefs.sortBy { it.index }
            val b = segRefs.getOrNull(0)?.index ?: 0
            for ((idx, ref) in segRefs.withIndex()) {
                if ((ref.index - b) != idx) {
                    throw Error("name is not sequential: ${ref.name}")
                }
            }
            return segRefs
        }

        fun getSegmentIndexRange(dir: Path): Pair<Int?, Int?> {
            val refs = list(dir)
            return refs.getOrNull(0)?.index to refs.getOrNull(refs.size)?.index
        }
    }


}

class Page() {
    val alloc: Int = 0
    val buf: ByteBuffer = ByteBuffer.allocate(pageSize) // FIXME: byte can represent -127 ~ 128
}

class Wal(
    val logger: Logger,
    val dir: Path,
) {
    val page = Page()

    init {
        try {
            Files.createDirectories(dir)
        } catch (e: FileAlreadyExistsException) {
            // just ignore
        } catch (e: IOException) {
            logger.error("failed to create dir: $e")
            throw e
        }

        val (_, maxIndex) = Segment.getSegmentIndexRange(dir)

        Segment(dir, (maxIndex ?: 0) + 1)
    }

    fun log(buf: UByteArray) {

    }
}


fun main() {
    System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")
    val logger = KotlinLogging.logger("tsdb")
    val series = "http_requests_total{job=\"prometheus\",group=\"canary\"}".toByteArray()

    Wal(
        logger = logger,
        dir = Path.of("./lll")
    )

    logger.debug { "hello" }
}