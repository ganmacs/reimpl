package com.ganmacs.wal

import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.name
import kotlin.streams.toList
import kotlin.test.assertEquals

internal class WalReaderTest {
    private val logger = KotlinLogging.logger("test")
    private val message = "hello_world_this_is_a_test{instance=i-xxxxxx,tag=111111,staging=test,id=xxxxxxxxxxxxx}"
    private val messageSizeToWrite = pageSize / (message.length + recordHeaderSize)
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

    @AfterEach
    fun tearDown() {
        File(tmpDir.toUri()).delete()
    }

    @Test
    fun `reads single segment`() {
        val wal = Wal(logger = logger, dir = tmpDir, segmentSize = pageSize)
        wal.log(listOf(message, message).map { it.toByteArray() })
        wal.close()

        val reader = WalReader(SegmentReader(listOf(Segment(tmpDir, 0))))
        val expected = message.toByteArray().toList()
        assertEquals(true, reader.hasNext())
        assertEquals(expected, reader.next().toList())
        assertEquals(true, reader.hasNext())
        assertEquals(expected, reader.next().toList())
        assertEquals(false, reader.hasNext())
    }

    @Test
    fun `read multiple segments`() {
        // create segment=2
        var wal = Wal(logger = logger, dir = tmpDir, segmentSize = pageSize)
        wal.log(listOf(message, message).map { it.toByteArray() })
        wal.close()

        // create segment=1
        wal = Wal(logger = logger, dir = tmpDir, segmentSize = pageSize)
        wal.log(listOf(message, message).map { it.toByteArray() })
        wal.close()

        val segments = listOf(Segment(tmpDir, 0), Segment(tmpDir, 1))
        val reader = WalReader(SegmentReader(segments))
        val expected = message.toByteArray().toList()
        assertEquals(expected, reader.next().toList())
        assertEquals(expected, reader.next().toList())
        assertEquals(expected, reader.next().toList())
        assertEquals(expected, reader.next().toList())
        assertEquals(false, reader.hasNext())
    }
}