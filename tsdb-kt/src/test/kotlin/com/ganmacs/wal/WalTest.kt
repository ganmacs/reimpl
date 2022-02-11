package com.ganmacs.wal

import mu.KotlinLogging
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.name
import kotlin.streams.toList
import kotlin.test.assertEquals


internal class WalTest {
    private val logger = KotlinLogging.logger("test")
    private lateinit var tmpDir: Path

    @BeforeEach
    fun setup() {
        try {
            val tmp = System.getProperty("java.io.tmpdir")
            tmpDir = Files.createTempDirectory(Paths.get(tmp), "wal-test")
        } catch (ex: IOException) {
            System.err.println(ex.message)
        }
    }

    @Nested
    inner class Log() {
        @Test
        fun `run log`() {
            val wal = Wal(logger = logger, dir = tmpDir, segmentSize = pageSize)
            val message = "hello_world_this_is_a_test{instance=i-xxxxxx,tag=111111,staging=test,id=xxxxxxxxxxxxx}"
            val messageSizeToWrite = pageSize / (message.length + recordHeaderSize)

            var l = mutableListOf<ByteArray>()
            for (x in 1..messageSizeToWrite) {
                l.add(message.toByteArray())
            }
            wal.log(l)
            val walFiles1: List<String> = Files.list(tmpDir).toList().map { it.fileName.name }
            assertEquals(listOf("00000000"), walFiles1)

            wal.log(l)
            val walFiles2: List<String> = Files.list(tmpDir).toList().map { it.fileName.name }.sorted()
            assertEquals(listOf("00000000", "00000001"), walFiles2)

            wal.close()
        }

        @Test
        fun `flush page per pageSize`() {
            var wal = Wal(logger = logger, dir = tmpDir, segmentSize = pageSize * 5)
            val message = "hello_world_this_is_a_test{instance=i-xxxxxx,tag=111111,staging=test,id=xxxxxxxxxxxxx}"
            val messageSizeToWrite = pageSize / (message.length + recordHeaderSize)

            var size = 0
            var l = mutableListOf<ByteArray>()
            for (x in 1..messageSizeToWrite) {
                l.add(message.toByteArray())
                size += message.length
            }

            wal.log(l)
            wal.close()
            assertEquals(
                pageSize.toLong(),
                Files.size(Path.of(tmpDir.toString(), "00000000"))
            )

            wal = Wal(logger = logger, dir = tmpDir, segmentSize = pageSize * 5)
            wal.log(l)
            wal.log(l)
            wal.close()
            assertEquals(
                pageSize.toLong() * 2,
                Files.size(Path.of(tmpDir.toString(), "00000001"))
            )
        }
    }
}
