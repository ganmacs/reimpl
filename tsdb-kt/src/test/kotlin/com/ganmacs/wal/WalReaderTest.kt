package com.ganmacs.wal

import com.ganmacs.util.SeqInputStreamReader
import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.Path
import kotlin.io.path.absolutePathString
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
    fun `empty segment`() {
        Segment.create(tmpDir, 0)
        val reader = WalReader(createSegmentReader(tmpDir))
        assertEquals(false, reader.hasNext())
    }

    @Test
    fun `reads single row`() {
        val wal = Wal(logger = logger, dir = tmpDir, segmentSize = pageSize)
        wal.log(listOf(message).map { it.toByteArray() })
        wal.close()

        assertEquals(1, listSegments(tmpDir).size)

        for (s in listSegments(tmpDir)) {
            val t = tmpDir.resolve(s.name)
            println(t)
            println(Files.size(t))
        }

        val reader = WalReader(createSegmentReader(tmpDir))
        val expected = message.toByteArray().toList()
        assertEquals(true, reader.hasNext())
        assertEquals(expected, reader.next().toList())
        assertEquals(false, reader.hasNext())
    }

    @Test
    fun `reads single segment`() {
        val wal = Wal(logger = logger, dir = tmpDir, segmentSize = pageSize)
        wal.log(listOf(message, message).map { it.toByteArray() })
        wal.close()

        assertEquals(1, listSegments(tmpDir).size)

        val reader = WalReader(createSegmentReader(tmpDir))
        val expected = message.toByteArray().toList()
        assertEquals(true, reader.hasNext())
        assertEquals(expected, reader.next().toList())
        assertEquals(true, reader.hasNext())
        assertEquals(expected, reader.next().toList())
        assertEquals(false, reader.hasNext())
    }

    @Test
    fun `large data`() {
        val wal = Wal(logger = logger, dir = tmpDir, segmentSize = defaultSegmentSize)
        val message = StringBuilder().also {
            for (i in 0..(messageSizeToWrite * 3)) {
                it.append(message)
            }
        }.toString()
        wal.log(listOf(message).map { it.toByteArray() })
        wal.close()

        assertEquals(1, listSegments(tmpDir).size)

        val reader = WalReader(createSegmentReader(tmpDir))
        val expected = message.toByteArray().toList()
        assertEquals(true, reader.hasNext())
        assertEquals(expected.size, reader.next().size)
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

        assertEquals(2, listSegments(tmpDir).size)

        val reader = WalReader(createSegmentReader(tmpDir))
        val expected = message.toByteArray().toList()
        assertEquals(expected, reader.next().toList())
        assertEquals(expected, reader.next().toList())
        assertEquals(expected, reader.next().toList())
        assertEquals(expected, reader.next().toList())
        assertEquals(false, reader.hasNext())
    }

    private fun createSegmentReader(dir: Path): SegmentBufReader {
        val ins = listSegments(tmpDir).map {
            val path = Path(dir.absolutePathString(), it.name).absolutePathString()
            FileInputStream(File(path))
        }
        return BufferedInputStream(SeqInputStreamReader(ins), pageSize * 16)
    }
}