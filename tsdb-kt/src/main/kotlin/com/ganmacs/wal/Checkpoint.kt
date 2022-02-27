package com.ganmacs.wal

import com.ganmacs.glog
import org.slf4j.Logger
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.isDirectory
import kotlin.streams.toList

private const val checkpointPrefix: String = "checkpoint."

internal fun performCheckpoint(logger: Logger, from: Int, to: Int, dir: Path, keep: (Int) -> Boolean) {
    logger.info("Creating checkpoint fromSegment=$from toSegment=$to")
    val lastRef = lastCheckpoint(dir) ?: return
    val lastIdx = lastRef.idx + 1
    if (from > lastIdx) {
        throw error("unexpected gap required=$from expected=$lastIdx")
    }
    println(lastRef.name)
    val reader = listOf(
        SegmentRange(dir.resolve(lastRef.name), 0, Int.MAX_VALUE), // checkpoint
        SegmentRange(dir, lastIdx, to), // target segments
    ).toSegmentReader()

    val nextCheckpoint = checkpointDir(dir, to)
    val tmpCheckpoint = Path.of("$nextCheckpoint.tmp")

    // remove tmp dir
    // re-create

    val wal = Wal(logger, tmpCheckpoint)

    val walIter = WalReader(reader)
    var buf = mutableListOf<ByteArray>()

    for (record in walIter) {
        if (keep(1)) { // TODO
            buf.add(record)
        }
        // flush per 1M
    }
    wal.log(buf)
    wal.close() // blocking. it's okay to rename here

    Files.move(tmpCheckpoint, nextCheckpoint)
}

internal fun deleteCheckpoint(dir: Path, index: Int) {
    for (c in listCheckpoints(dir)) {
        if (c.idx >= index) continue

        val p = dir.resolve(c.name)
        p.toFile().deleteRecursively()
        glog.debug("deleted checkpoint dir: $p")
    }
}

private fun checkpointDir(path: Path, i: Int): Path = path.resolve(String.format("${checkpointPrefix}%08d", i))

internal data class CheckpointRef(val name: String, val idx: Int)

internal fun lastCheckpoint(dir: Path): CheckpointRef? = try {
    listCheckpoints(dir).last()
} catch (e: NoSuchElementException) {
    null
}

internal fun listCheckpoints(dir: Path): List<CheckpointRef> =
    Files.list(dir).toList().mapNotNull {
        val fileName = it.fileName.toString()
        println(fileName)
        if (!fileName.startsWith(checkpointPrefix)) return@mapNotNull null

        if (!it.isDirectory()) throw error("checkpoint is not directory")

        val index = try {
            Integer.parseInt(fileName.removePrefix(checkpointPrefix))
        } catch (e: NumberFormatException) {
            glog.warn { "checkpoint file name: $fileName is invalid. it must be number" }
            return@mapNotNull null
        }
        CheckpointRef(fileName, index)
    }.sortedBy { it.idx }