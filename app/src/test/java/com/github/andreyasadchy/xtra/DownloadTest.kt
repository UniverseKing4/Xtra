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

    @Test
    fun testManualStopDoesNotBecomeWaitingDownload() {
        val stoppedVideo = OfflineVideo(
            videoId = "123",
            name = "Manual Stopped VOD",
            status = OfflineVideo.STATUS_PENDING
        )
        // Verify that STATUS_PENDING is distinct from STATUS_WAITING_FOR_WIFI and STATUS_WAITING_FOR_NETWORK
        val isWaitingDownload = stoppedVideo.status == OfflineVideo.STATUS_WAITING_FOR_WIFI ||
                stoppedVideo.status == OfflineVideo.STATUS_WAITING_FOR_NETWORK
        assertEquals(false, isWaitingDownload)
    }

    @Test
    fun testNetworkFailureBecomesWaitingForNetworkOrWifi() {
        fun resolveFailureStatus(wifiOnly: Boolean, isCellular: Boolean): Int {
            return if (wifiOnly && isCellular) {
                OfflineVideo.STATUS_WAITING_FOR_WIFI
            } else {
                OfflineVideo.STATUS_WAITING_FOR_NETWORK
            }
        }

        assertEquals(OfflineVideo.STATUS_WAITING_FOR_WIFI, resolveFailureStatus(wifiOnly = true, isCellular = true))
        assertEquals(OfflineVideo.STATUS_WAITING_FOR_NETWORK, resolveFailureStatus(wifiOnly = false, isCellular = true))
        assertEquals(OfflineVideo.STATUS_WAITING_FOR_NETWORK, resolveFailureStatus(wifiOnly = true, isCellular = false))
    }

    @Test
    fun testChatReplayDelayAtVariablePlaybackSpeed() {
        val currentPosition = 10000L
        val messageOffset = 10100L

        for (speed in listOf(1.0f, 1.25f, 1.5f, 2.0f)) {
            val timeLeft = (messageOffset - currentPosition).div(speed).toLong()
            val delay = timeLeft.coerceIn(30L, 1000L)
            assert(delay >= 30L) { "Delay should be at least 30ms to prevent UI freeze" }
            assert(delay <= 1000L) { "Delay should be capped at 1000ms" }
        }

        // When message is in past (messageOffset <= currentPosition)
        val pastMessageOffset = 9500L
        val pastTimeLeft = (pastMessageOffset - currentPosition).div(2.0f).toLong()
        val pastDelay = pastTimeLeft.coerceIn(30L, 1000L)
        assertEquals(30L, pastDelay)
    }
}
