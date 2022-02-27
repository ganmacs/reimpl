package com.ganmacs.wal

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.test.assertEquals

internal class SegmentTest {
    private lateinit var tmpDir: Path

    @BeforeEach
    fun setup() {
        try {
            val tmp = System.getProperty("java.io.tmpdir")
            tmpDir = Files.createTempDirectory(Paths.get(tmp), "segment-test")
        } catch (ex: IOException) {
            System.err.println(ex.message)
        }
    }

    @AfterEach
    fun tearDown() {
        File(tmpDir.toUri()).delete()
    }

    @Nested
    inner class ListSegment {
        @Test
        fun `lists segments`() {
            Segment.create(tmpDir, 1)
            Segment.create(tmpDir, 2)

            assertEquals(
                listOf(
                    SegmentRef("00000001", 1),
                    SegmentRef("00000002", 2),
                ),
                listSegments(tmpDir)
            )

            Segment.create(tmpDir, 4)
            val err = assertThrows<Error> { listSegments(tmpDir) }
            assertEquals("name is not sequential: 00000004", err.message)
        }
    }
}