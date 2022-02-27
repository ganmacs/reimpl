package com.ganmacs.wal

import mu.KotlinLogging
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.streams.toList
import kotlin.test.assertEquals

class CheckpointTest {
    private lateinit var tmpDir: Path
    private val logger = KotlinLogging.logger("test")

    @BeforeEach
    fun setup() {
        try {
            val tmp = System.getProperty("java.io.tmpdir")
            tmpDir = Files.createTempDirectory(Paths.get(tmp), "wal-chechpoint-test")
        } catch (ex: IOException) {
            System.err.println(ex.message)
        }
    }

    @AfterEach
    fun tearDown() {
        File(tmpDir.toUri()).delete()
    }

    @Nested
    inner class LastCheckpoint {
        @Test
        fun `return last checkpoint`() {
            assertEquals(null, lastCheckpoint(tmpDir))

            val c1 = "checkpoint.00000001"
            Files.createDirectories(tmpDir.resolve(c1))
            assertEquals(
                CheckpointRef(c1, 1),
                lastCheckpoint(tmpDir),
            )

            val c2 = "checkpoint.00000100"
            Files.createDirectories(tmpDir.resolve(c2))
            assertEquals(
                CheckpointRef(c2, 100),
                lastCheckpoint(tmpDir),
            )

            Files.createDirectories(tmpDir.resolve("checkpoint.00000010"))
            assertEquals(
                CheckpointRef(c2, 100),
                lastCheckpoint(tmpDir),
            )
        }
    }


    private val message = "hello_world_this_is_a_test{instance=i-xxxxxx,tag=111111,staging=test,id=xxxxxxxxxxxxx}"

    @Test
    fun `checkpoint`() {
        // create dummy
        val segment = Segment.create(tmpDir, 100)
        segment.close()

        val cwal = Wal(logger, tmpDir.resolve("checkpoint.0099"))
        cwal.log(listOf(message.toByteArray()))
        cwal.close()

        val wal = Wal(logger, tmpDir, pageSize * 2)
        var n = 0
        while (n < 104) {
            wal.log(listOf(message.toByteArray()))
            n = listSegments(tmpDir).last().index
        }
        wal.close()

        var i = 0
        performCheckpoint(logger, 100, 104, tmpDir) { n -> (i++ % 2) == 0 }
        wal.truncate(105)
        println(Files.list(tmpDir).toList())
        deleteCheckpoint(tmpDir, 104)

        assertEquals(
            listOf(tmpDir.resolve("checkpoint.00000104")),
            Files.list(tmpDir).toList()
        )
    }
}