package com.github.andreyasadchy.xtra

import com.github.andreyasadchy.xtra.model.ui.DownloadProgress
import com.github.andreyasadchy.xtra.model.ui.OfflineVideo
import com.github.andreyasadchy.xtra.util.m3u8.PlaylistUtils
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.RandomAccessFile
import java.util.concurrent.ConcurrentHashMap

class DownloadTest {

    @Test
    fun testPlaylistParsing() {
        val m3u8 = """
            #EXTM3U
            #EXT-X-VERSION:3
            #EXT-X-TARGETDURATION:4
            #EXTINF:4.000,
            index-0.ts
            #EXTINF:4.000,
            index-1.ts
            #EXTINF:4.000,
            index-2.ts
            #EXT-X-ENDLIST
        """.trimIndent()

        val playlist = PlaylistUtils.parseMediaPlaylist(ByteArrayInputStream(m3u8.toByteArray()))
        assertEquals(3, playlist.segments.size)
        assertEquals("index-0.ts", playlist.segments[0].uri)
        assertEquals("index-1.ts", playlist.segments[1].uri)
        assertEquals("index-2.ts", playlist.segments[2].uri)
    }

    @Test
    fun testOrderedSegmentAssemblerPipeline() {
        runBlocking {
            val totalSegments = 50
            val segmentPayloads = (0 until totalSegments).map { i ->
                "SegmentData_$i".toByteArray()
            }

            val readySegments = ConcurrentHashMap<Int, ByteArray>()
            val segmentAvailableChannel = Channel<Unit>(Channel.CONFLATED)
            val outputStream = ByteArrayOutputStream()

            coroutineScope {
                val writerJob = launch {
                    var nextIndex = 0
                    while (nextIndex < totalSegments) {
                        val bytes = readySegments.remove(nextIndex)
                        if (bytes != null) {
                            outputStream.write(bytes)
                            nextIndex++
                        } else {
                            segmentAvailableChannel.receive()
                        }
                    }
                }

                val indices = (0 until totalSegments).shuffled()
                for (idx in indices) {
                    launch {
                        readySegments[idx] = segmentPayloads[idx]
                        segmentAvailableChannel.trySend(Unit)
                    }
                }

                writerJob.join()
            }

            val expectedAllBytes = ByteArrayOutputStream().apply {
                for (payload in segmentPayloads) {
                    write(payload)
                }
            }.toByteArray()

            assertArrayEquals(expectedAllBytes, outputStream.toByteArray())
        }
    }

    @Test
    fun testResumeTruncationAndAppend() {
        val tempFile = File.createTempFile("xtra_test_download", ".bin").apply { deleteOnExit() }

        val initialBytes = ByteArray(100) { it.toByte() }
        FileOutputStream(tempFile).use { it.write(initialBytes) }
        assertEquals(100L, tempFile.length())

        RandomAccessFile(tempFile, "rw").use { it.setLength(60L) }
        assertEquals(60L, tempFile.length())

        val appendBytes = ByteArray(40) { (60 + it).toByte() }
        FileOutputStream(tempFile, true).use { it.write(appendBytes) }
        assertEquals(100L, tempFile.length())

        val resultBytes = tempFile.readBytes()
        assertArrayEquals(initialBytes, resultBytes)
    }

    @Test
    fun testDownloadProgressCalculation() {
        val progress = DownloadProgress(
            id = 1,
            progress = 25,
            maxProgress = 100,
            chatProgress = 50,
            maxChatProgress = 100,
            bytes = 1024L * 1024L * 10L,
            chatBytes = 5000L
        )

        val videoPercent = ((progress.progress.toFloat() / progress.maxProgress) * 100f).toInt().coerceIn(0, 100)
        assertEquals(25, videoPercent)

        val chatPercent = ((progress.chatProgress.toFloat() / progress.maxChatProgress) * 100f).toInt().coerceIn(0, 100)
        assertEquals(50, chatPercent)

        val chatOnlyProgress = DownloadProgress(
            id = 2,
            progress = 0,
            maxProgress = 100,
            chatProgress = 80,
            maxChatProgress = 100,
            bytes = 0L,
            chatBytes = 12000L
        )
        val chatOnlyPercent = ((chatOnlyProgress.chatProgress.toFloat() / chatOnlyProgress.maxChatProgress) * 100f).toInt().coerceIn(0, 100)
        assertEquals(80, chatOnlyPercent)
    }

    @Test
    fun testOfflineVideoStatusTransitions() {
        val video = OfflineVideo(
            videoId = "123456789",
            name = "Test Stream VOD",
            quality = "1080p60",
            downloadChat = true,
            status = OfflineVideo.STATUS_PENDING
        )
        assertEquals(OfflineVideo.STATUS_PENDING, video.status)

        video.status = OfflineVideo.STATUS_DOWNLOADING
        assertEquals(OfflineVideo.STATUS_DOWNLOADING, video.status)

        video.status = OfflineVideo.STATUS_DOWNLOADED
        assertEquals(OfflineVideo.STATUS_DOWNLOADED, video.status)
    }
}
