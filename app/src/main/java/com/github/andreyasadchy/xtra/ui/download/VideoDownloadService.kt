package com.github.andreyasadchy.xtra.ui.download

import android.annotation.SuppressLint
import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.ContentResolver
import android.content.Intent
import android.content.pm.ServiceInfo
import android.graphics.drawable.Icon
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.os.Binder
import android.os.Build
import android.os.IBinder
import android.provider.DocumentsContract
import android.util.Base64
import android.util.JsonReader
import android.util.JsonToken
import android.util.Log
import androidx.core.content.ContextCompat
import androidx.core.net.toUri
import androidx.lifecycle.LifecycleService
import androidx.lifecycle.lifecycleScope
import com.github.andreyasadchy.xtra.R
import com.github.andreyasadchy.xtra.XtraApp
import com.github.andreyasadchy.xtra.XtraModule
import com.github.andreyasadchy.xtra.model.chat.CheerEmote
import com.github.andreyasadchy.xtra.model.chat.Emote
import com.github.andreyasadchy.xtra.model.chat.TwitchBadge
import com.github.andreyasadchy.xtra.model.chat.TwitchEmote
import com.github.andreyasadchy.xtra.model.ui.DownloadProgress
import com.github.andreyasadchy.xtra.model.ui.OfflineVideo
import com.github.andreyasadchy.xtra.ui.main.MainActivity
import com.github.andreyasadchy.xtra.util.C
import com.github.andreyasadchy.xtra.util.NetworkUtils
import com.github.andreyasadchy.xtra.util.NetworkUtils.executeAsync
import com.github.andreyasadchy.xtra.util.TwitchApiHelper
import com.github.andreyasadchy.xtra.util.SafUtils
import com.github.andreyasadchy.xtra.util.m3u8.MediaPlaylist
import com.github.andreyasadchy.xtra.util.m3u8.PlaylistUtils
import com.github.andreyasadchy.xtra.util.m3u8.Segment
import com.github.andreyasadchy.xtra.util.prefs
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import okhttp3.Request
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStream
import java.io.RandomAccessFile
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.coroutines.cancellation.CancellationException

class VideoDownloadService : LifecycleService() {

    lateinit var xtraModule: XtraModule
    private val okHttpClient = lazy {
        xtraModule.okHttpClient.value.newBuilder().apply {
            connectTimeout(5, TimeUnit.MINUTES)
            writeTimeout(5, TimeUnit.MINUTES)
            readTimeout(5, TimeUnit.MINUTES)
        }.build()
    }

    private var notificationManager: NotificationManager? = null
    private lateinit var downloadSemaphore: Semaphore
    private val downloadJobs = mutableMapOf<Int, Job>()
    private val offlineVideos = mutableListOf<OfflineVideo>()
    val activeDownloads = mutableListOf<DownloadProgress>()
    var listener: Listener? = null

    interface Listener {
        fun update(downloadProgress: DownloadProgress)
        fun unbind()
    }

    override fun onCreate() {
        super.onCreate()
        xtraModule = (application as XtraApp).xtraModule
        downloadSemaphore = Semaphore(prefs().getInt(C.DOWNLOAD_LIMIT, 2))
    }

    private fun start(videoId: Int) {
        if (activeDownloads.find { it.id == videoId } == null) {
            lifecycleScope.launch(Dispatchers.IO) {
                val offlineVideo = xtraModule.offlineVideosRepository.getById(videoId)
                if (offlineVideo != null) {
                    val downloadProgress = DownloadProgress(
                        id = videoId,
                        progress = offlineVideo.progress,
                        maxProgress = offlineVideo.maxProgress,
                        bytes = offlineVideo.bytes,
                        chatProgress = offlineVideo.chatProgress,
                        maxChatProgress = offlineVideo.maxChatProgress,
                        chatBytes = offlineVideo.chatBytes,
                        chatOffsetSeconds = offlineVideo.chatOffsetSeconds,
                    )
                    offlineVideos.add(offlineVideo)
                    activeDownloads.add(downloadProgress)

                    notificationManager = getSystemService(NOTIFICATION_SERVICE) as NotificationManager
                    val channelId = getString(R.string.notification_downloads_channel_id)
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O && notificationManager?.getNotificationChannel(channelId) == null) {
                        notificationManager?.createNotificationChannel(
                            NotificationChannel(
                                channelId,
                                ContextCompat.getString(this@VideoDownloadService, R.string.notification_downloads_channel_title),
                                NotificationManager.IMPORTANCE_DEFAULT
                            ).apply {
                                setSound(null, null)
                            }
                        )
                    }
                    sendNotification(offlineVideo, downloadProgress)

                    if (downloadSemaphore.availablePermits <= 0) {
                        xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                            status = OfflineVideo.STATUS_QUEUED
                        })
                    }
                    downloadSemaphore.withPermit {
                        xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                            status = OfflineVideo.STATUS_DOWNLOADING
                        })
                        sendNotification(offlineVideo, downloadProgress)
                        var retriesLeft = prefs().getString(C.DOWNLOAD_AUTO_RETRY_COUNT, "3")?.toIntOrNull() ?: 3
                        val autoRetry = prefs().getBoolean(C.DOWNLOAD_AUTO_RETRY, true)
                        while (true) {
                            try {
                                if (offlineVideo.quality == "chat_only") {
                                    startChatJob(offlineVideo, downloadProgress, offlineVideo.downloadPath!!)
                                } else {
                                    var sourceUrl = offlineVideo.sourceUrl
                                    val networkLibrary = prefs().getString(C.NETWORK_LIBRARY, C.OKHTTP)
                                    if (sourceUrl.isNullOrBlank() && !offlineVideo.videoId.isNullOrBlank()) {
                                        sourceUrl = refreshVideoSourceUrl(offlineVideo, networkLibrary)
                                    }
                                    if (sourceUrl.isNullOrBlank()) {
                                        throw IllegalStateException("Source URL is null or empty")
                                    }
                                    if (sourceUrl.endsWith(".m3u8")) {
                                        try {
                                            downloadVideo(offlineVideo, downloadProgress, sourceUrl)
                                        } catch (e: Exception) {
                                            if (e !is CancellationException && autoRetry && retriesLeft > 0) {
                                                val refreshed = refreshVideoSourceUrl(offlineVideo, networkLibrary)
                                                if (!refreshed.isNullOrBlank()) {
                                                    offlineVideo.sourceUrl = refreshed
                                                }
                                            }
                                            throw e
                                        }
                                    } else {
                                        downloadClip(offlineVideo, downloadProgress, sourceUrl)
                                    }
                                }
                                break
                            } catch (e: CancellationException) {
                                ensureActive()
                                break
                            } catch (e: Exception) {
                                Log.e("VideoDownloadService", "Download failed for video $videoId", e)
                                if (autoRetry && retriesLeft > 0) {
                                    retriesLeft--
                                    delay(5000L)
                                } else {
                                    break
                                }
                            }
                        }
                        offlineVideos.remove(offlineVideo)
                        activeDownloads.remove(downloadProgress)
                        val done = (offlineVideo.quality == "chat_only" || downloadProgress.progress >= downloadProgress.maxProgress) &&
                                (!offlineVideo.downloadChat || downloadProgress.chatProgress >= downloadProgress.maxChatProgress)
                        xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                            status = if (done) {
                                OfflineVideo.STATUS_DOWNLOADED
                            } else {
                                val waitForWifi = if (prefs().getBoolean(C.DOWNLOAD_WIFI_ONLY, false)) {
                                    val connectivityManager = getSystemService(CONNECTIVITY_SERVICE) as ConnectivityManager
                                    val networkCapabilities = connectivityManager.getNetworkCapabilities(connectivityManager.activeNetwork)
                                    networkCapabilities != null && networkCapabilities.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR)
                                } else false
                                if (waitForWifi) {
                                    OfflineVideo.STATUS_WAITING_FOR_WIFI
                                } else {
                                    OfflineVideo.STATUS_PENDING
                                }
                            }
                            if (status != OfflineVideo.STATUS_DOWNLOADED && prefs().getBoolean(C.DOWNLOAD_AUTO_RETRY, true)) {
                                DownloadRetryWorker.enqueueRetry(applicationContext, 15L)
                            }
                            progress = downloadProgress.progress
                            maxProgress = downloadProgress.maxProgress
                            bytes = downloadProgress.bytes
                            chatProgress = downloadProgress.chatProgress
                            maxChatProgress = downloadProgress.maxChatProgress
                            chatBytes = downloadProgress.chatBytes
                            chatOffsetSeconds = downloadProgress.chatOffsetSeconds
                        })
                        if (done) {
                            val notification = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                                Notification.Builder(this@VideoDownloadService, getString(R.string.notification_downloads_channel_id))
                            } else {
                                @Suppress("DEPRECATION")
                                Notification.Builder(this@VideoDownloadService)
                            }.apply {
                                setContentTitle(ContextCompat.getString(this@VideoDownloadService, R.string.downloaded))
                                setContentText(offlineVideo.name)
                                setSmallIcon(android.R.drawable.stat_sys_download_done)
                                setGroup(GROUP_KEY)
                                setAutoCancel(true)
                                setContentIntent(
                                    PendingIntent.getActivity(
                                        this@VideoDownloadService,
                                        -offlineVideo.id,
                                        Intent(this@VideoDownloadService, MainActivity::class.java).apply {
                                            flags = Intent.FLAG_ACTIVITY_CLEAR_TOP
                                            action = MainActivity.INTENT_OPEN_DOWNLOADED_VIDEO
                                            putExtra(MainActivity.KEY_VIDEO, offlineVideo)
                                        },
                                        PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
                                    )
                                )
                            }.build()
                            notificationManager?.notify(-offlineVideo.id, notification)
                        }
                        val nextOfflineVideo = offlineVideos.firstOrNull()
                        val nextDownload = activeDownloads.firstOrNull()
                        if (nextOfflineVideo != null && nextDownload != null) {
                            sendNotification(nextOfflineVideo, nextDownload)
                            notificationManager?.cancel(videoId)
                        } else {
                            listener?.unbind()
                            stopSelf()
                        }
                    }
                }
            }.also {
                it.invokeOnCompletion {
                    downloadJobs.remove(videoId)
                }
                downloadJobs[videoId] = it
            }
        }
    }

    private suspend fun refreshVideoSourceUrl(offlineVideo: OfflineVideo, networkLibrary: String?): String? = withContext(Dispatchers.IO) {
        val videoId = offlineVideo.videoId ?: return@withContext null
        try {
            val gqlHeaders = TwitchApiHelper.getGQLHeaders(this@VideoDownloadService, prefs().getBoolean(C.TOKEN_INCLUDE_TOKEN_VIDEO, true))
            val playerType = prefs().getString(C.TOKEN_PLAYER_TYPE_VIDEO, "channel_home_live")
            val supportedCodecs = prefs().getString(C.TOKEN_SUPPORTED_CODECS, "av1,h265,h264")
            val enableIntegrity = prefs().getBoolean(C.ENABLE_INTEGRITY, false)
            val result = xtraModule.playerRepository.loadVideoPlaylistUrl(networkLibrary, gqlHeaders, videoId, playerType, supportedCodecs, enableIntegrity)
            val masterPlaylistUrl = result.first
            val playlistText = try {
                downloadByteArray(networkLibrary, masterPlaylistUrl).decodeToString()
            } catch (e: Exception) {
                null
            }
            if (!playlistText.isNullOrBlank()) {
                val stableVariantIds = Regex("STABLE-VARIANT-ID=\"(.+?)\"").findAll(playlistText).mapNotNull { it.groups[1]?.value }.toList()
                val urls = Regex("https://.*\\.m3u8").findAll(playlistText).map(MatchResult::value).toList()
                val targetQuality = offlineVideo.quality ?: "source"
                var matchedUrl: String? = null
                stableVariantIds.forEachIndexed { index, variantId ->
                    if (variantId.equals(targetQuality, ignoreCase = true) ||
                        (targetQuality == "source" && variantId.equals("chunked", ignoreCase = true)) ||
                        (targetQuality.startsWith("audio", ignoreCase = true) && variantId.equals("audio_only", ignoreCase = true))
                    ) {
                        matchedUrl = urls.getOrNull(index)
                    }
                }
                if (matchedUrl == null) {
                    matchedUrl = urls.find { it.contains(targetQuality, ignoreCase = true) } ?: urls.firstOrNull()
                }
                if (!matchedUrl.isNullOrBlank()) {
                    offlineVideo.sourceUrl = matchedUrl
                    xtraModule.offlineVideosRepository.update(offlineVideo)
                    return@withContext matchedUrl
                }
            }
        } catch (e: Exception) {
            Log.e("VideoDownloadService", "Error refreshing VOD playlist URL", e)
        }
        return@withContext null
    }

    private suspend fun downloadByteArray(networkLibrary: String?, url: String): ByteArray = withContext(Dispatchers.IO) {
        val userAgent = "Xtra/" + com.github.andreyasadchy.xtra.BuildConfig.VERSION_NAME
        when {
            networkLibrary == C.HTTP_ENGINE && xtraModule.httpEngine.value != null -> @SuppressLint("NewApi") {
                val response = suspendCancellableCoroutine { continuation ->
                    val timeout = NetworkUtils.HttpEngineTimeout(CRONET_TIMEOUT)
                    val request = xtraModule.httpEngine.value!!.newUrlRequestBuilder(
                        url,
                        xtraModule.cronetExecutor.value,
                        NetworkUtils.ByteArrayUrlCallback(continuation, timeout)
                    ).addHeader("User-Agent", userAgent).build()
                    timeout.start(request, continuation)
                    request.start()
                    continuation.invokeOnCancellation {
                        request.cancel()
                        timeout.stop()
                    }
                }
                if (response.info.httpStatusCode in 200..299) {
                    response.body
                } else {
                    throw IOException("HTTP error code: ${response.info.httpStatusCode} for $url")
                }
            }
            networkLibrary == C.CRONET && xtraModule.cronetEngine.value != null -> {
                val response = suspendCancellableCoroutine { continuation ->
                    val timeout = NetworkUtils.CronetTimeout(CRONET_TIMEOUT)
                    val request = xtraModule.cronetEngine.value!!.newUrlRequestBuilder(
                        url,
                        NetworkUtils.ByteArrayCronetCallback(continuation, timeout),
                        xtraModule.cronetExecutor.value
                    ).addHeader("User-Agent", userAgent).build()
                    timeout.start(request, continuation)
                    request.start()
                    continuation.invokeOnCancellation {
                        request.cancel()
                        timeout.stop()
                    }
                }
                if (response.info.httpStatusCode in 200..299) {
                    response.body
                } else {
                    throw IOException("HTTP error code: ${response.info.httpStatusCode} for $url")
                }
            }
            else -> {
                okHttpClient.value.newCall(
                    Request.Builder()
                        .url(url)
                        .header("User-Agent", userAgent)
                        .build()
                ).executeAsync().use { response ->
                    if (response.isSuccessful) {
                        response.body.source().readByteArray()
                    } else {
                        throw IOException("HTTP error code: ${response.code} for $url")
                    }
                }
            }
        }
    }

    private suspend fun downloadToStream(
        networkLibrary: String?,
        url: String,
        outputStream: OutputStream,
        onProgress: ((bytesRead: Long, totalBytes: Long) -> Unit)? = null
    ) = withContext(Dispatchers.IO) {
        when {
            networkLibrary == C.HTTP_ENGINE && xtraModule.httpEngine.value != null -> @SuppressLint("NewApi") {
                val response = suspendCancellableCoroutine { continuation ->
                    val timeout = NetworkUtils.HttpEngineTimeout(CRONET_TIMEOUT)
                    val request = xtraModule.httpEngine.value!!.newUrlRequestBuilder(
                        url,
                        xtraModule.cronetExecutor.value,
                        NetworkUtils.ByteArrayUrlCallback(continuation, timeout)
                    ).build()
                    timeout.start(request, continuation)
                    request.start()
                    continuation.invokeOnCancellation {
                        request.cancel()
                        timeout.stop()
                    }
                }
                if (response.info.httpStatusCode in 200..299) {
                    outputStream.write(response.body)
                    onProgress?.invoke(response.body.size.toLong(), response.body.size.toLong())
                } else {
                    throw IOException("HTTP error code: ${response.info.httpStatusCode} for $url")
                }
            }
            networkLibrary == C.CRONET && xtraModule.cronetEngine.value != null -> {
                val response = suspendCancellableCoroutine { continuation ->
                    val timeout = NetworkUtils.CronetTimeout(CRONET_TIMEOUT)
                    val request = xtraModule.cronetEngine.value!!.newUrlRequestBuilder(
                        url,
                        NetworkUtils.ByteArrayCronetCallback(continuation, timeout),
                        xtraModule.cronetExecutor.value
                    ).build()
                    timeout.start(request, continuation)
                    request.start()
                    continuation.invokeOnCancellation {
                        request.cancel()
                        timeout.stop()
                    }
                }
                if (response.info.httpStatusCode in 200..299) {
                    outputStream.write(response.body)
                    onProgress?.invoke(response.body.size.toLong(), response.body.size.toLong())
                } else {
                    throw IOException("HTTP error code: ${response.info.httpStatusCode} for $url")
                }
            }
            else -> {
                okHttpClient.value.newCall(Request.Builder().url(url).build()).executeAsync().use { response ->
                    if (response.isSuccessful) {
                        val total = response.body.contentLength()
                        val source = response.body.source()
                        val buffer = ByteArray(64 * 1024)
                        var totalRead = 0L
                        var read: Int
                        while (source.read(buffer).also { read = it } != -1) {
                            outputStream.write(buffer, 0, read)
                            totalRead += read
                            onProgress?.invoke(totalRead, total)
                        }
                    } else {
                        throw IOException("HTTP error code: ${response.code} for $url")
                    }
                }
            }
        }
    }

    private suspend fun downloadVideo(offlineVideo: OfflineVideo, downloadProgress: DownloadProgress, sourceUrl: String) = withContext(Dispatchers.IO) {
        val networkLibrary = prefs().getString(C.NETWORK_LIBRARY, C.OKHTTP)
        val path = offlineVideo.downloadPath!!
        val from = offlineVideo.fromTime ?: 0L
        val to = offlineVideo.toTime ?: Long.MAX_VALUE
        val playlistBytes = downloadByteArray(networkLibrary, sourceUrl)
        val playlist = playlistBytes.inputStream().use {
            PlaylistUtils.parseMediaPlaylist(it)
        }
        val segments = mutableListOf<Segment>()
        var totalDuration = 0L
        var downloadDuration = 0L
        var startPosition = -1L
        for (segment in playlist.segments) {
            val startTime = totalDuration
            val duration = (segment.duration * 1000f).toLong()
            val endTime = startTime + duration
            if (endTime <= from) {
                totalDuration = endTime
            } else {
                if (startTime < to) {
                    segments.add(segment.copy(uri = segment.uri.replace("-unmuted", "-muted")))
                    totalDuration = endTime
                    downloadDuration += duration
                    if (startPosition == -1L) {
                        startPosition = startTime
                    }
                } else {
                    break
                }
            }
        }
        if (offlineVideo.duration == null) {
            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                duration = downloadDuration
                sourceStartPosition = startPosition
                maxProgress = segments.size
            })
            downloadProgress.maxProgress = offlineVideo.maxProgress
        }
        val urlPath = sourceUrl.substringBeforeLast('/') + "/"
        if (offlineVideo.playlistToFile) {
            downloadPlaylistToFile(offlineVideo, downloadProgress, networkLibrary, urlPath, path, playlist, segments)
        } else {
            downloadPlaylist(offlineVideo, downloadProgress, networkLibrary, urlPath, path, playlist, segments)
        }
    }

    private suspend fun downloadPlaylistToFile(
        offlineVideo: OfflineVideo,
        downloadProgress: DownloadProgress,
        networkLibrary: String?,
        urlPath: String,
        path: String,
        playlist: MediaPlaylist,
        segments: List<Segment>
    ) = withContext(Dispatchers.IO) {
        val videoFileUri = if (!offlineVideo.url.isNullOrBlank() && SafUtils.fileExists(contentResolver, offlineVideo.url!!)) {
            val fileUri = offlineVideo.url!!
            SafUtils.truncateFile(contentResolver, fileUri, downloadProgress.bytes)
            fileUri
        } else {
            val fileName = "${offlineVideo.videoId ?: ""}${offlineVideo.quality ?: ""}${offlineVideo.downloadDate}.${segments.firstOrNull()?.uri?.substringAfterLast(".") ?: "ts"}"
            val fileUri = SafUtils.getOrCreateDocument(contentResolver, path, fileName, "video/mp2t")
            var initSegmentBytes: Long? = null
            if (playlist.initSegmentUri != null) {
                val initUrl = if (playlist.initSegmentUri.startsWith("http://") || playlist.initSegmentUri.startsWith("https://")) {
                    playlist.initSegmentUri
                } else {
                    urlPath + playlist.initSegmentUri
                }
                val initData = downloadByteArray(networkLibrary, initUrl)
                SafUtils.openOutputStream(contentResolver, fileUri, append = true).use {
                    it.write(initData)
                }
                initSegmentBytes = initData.size.toLong()
            }
            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                url = fileUri
                initSegmentBytes?.let {
                    bytes += it
                    downloadProgress.bytes += it
                }
            })
            fileUri
        }

        downloadProgress.lastSaved = System.currentTimeMillis()

        coroutineScope {
            var chatJob: Job? = null
            if (offlineVideo.downloadChat && downloadProgress.chatProgress < downloadProgress.maxChatProgress) {
                chatJob = launch(Dispatchers.IO) {
                    try {
                        startChatJob(offlineVideo, downloadProgress, path)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        Log.e("VideoDownloadService", "Chat download failed", e)
                    }
                }
            }

            val requestSemaphore = Semaphore(prefs().getInt(C.DOWNLOAD_CONCURRENT_LIMIT, 10))
            val readySegments = ConcurrentHashMap<Int, ByteArray>()
            val segmentAvailableChannel = Channel<Unit>(Channel.CONFLATED)

            val writerJob = launch(Dispatchers.IO) {
                var nextIndex = downloadProgress.progress
                while (nextIndex < segments.size) {
                    val bytes = readySegments.remove(nextIndex)
                    if (bytes != null) {
                        SafUtils.openOutputStream(contentResolver, videoFileUri, append = true).use {
                            it.write(bytes)
                        }
                        downloadProgress.bytes += bytes.size
                        downloadProgress.progress += 1
                        listener?.update(downloadProgress)
                        sendNotification(offlineVideo, downloadProgress)

                        val currentTime = System.currentTimeMillis()
                        if (currentTime - downloadProgress.lastSaved >= 5000L) {
                            downloadProgress.lastSaved = currentTime
                            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                                progress = downloadProgress.progress
                                maxProgress = downloadProgress.maxProgress
                                this.bytes = downloadProgress.bytes
                                chatProgress = downloadProgress.chatProgress
                                maxChatProgress = downloadProgress.maxChatProgress
                                chatBytes = downloadProgress.chatBytes
                                chatOffsetSeconds = downloadProgress.chatOffsetSeconds
                            })
                        }
                        nextIndex++
                    } else {
                        segmentAvailableChannel.receive()
                    }
                }
            }

            val downloadJobs = (downloadProgress.progress until segments.size).map { index ->
                val segment = segments[index]
                launch(Dispatchers.IO) {
                    requestSemaphore.acquire()
                    try {
                        val segmentUrl = if (segment.uri.startsWith("http://") || segment.uri.startsWith("https://")) {
                            segment.uri
                        } else {
                            urlPath + segment.uri
                        }
                        var segmentBytes: ByteArray? = null
                        var segmentRetries = 3
                        while (segmentBytes == null && segmentRetries > 0) {
                            try {
                                segmentBytes = downloadByteArray(networkLibrary, segmentUrl)
                            } catch (e: CancellationException) {
                                throw e
                            } catch (e: Exception) {
                                segmentRetries--
                                if (segmentRetries > 0) {
                                    delay(1000L)
                                } else {
                                    if (segmentUrl.contains("-muted")) {
                                        try {
                                            segmentBytes = downloadByteArray(networkLibrary, segmentUrl.replace("-muted", "-unmuted"))
                                        } catch (e2: Exception) {
                                            try {
                                                segmentBytes = downloadByteArray(networkLibrary, segmentUrl.replace("-muted", ""))
                                            } catch (e3: Exception) {
                                                throw e
                                            }
                                        }
                                    } else if (segmentUrl.contains("-unmuted")) {
                                        try {
                                            segmentBytes = downloadByteArray(networkLibrary, segmentUrl.replace("-unmuted", "-muted"))
                                        } catch (e2: Exception) {
                                            try {
                                                segmentBytes = downloadByteArray(networkLibrary, segmentUrl.replace("-unmuted", ""))
                                            } catch (e3: Exception) {
                                                throw e
                                            }
                                        }
                                    } else {
                                        throw e
                                    }
                                }
                            }
                        }
                        if (segmentBytes != null) {
                            readySegments[index] = segmentBytes
                            segmentAvailableChannel.trySend(Unit)
                        }
                    } finally {
                        requestSemaphore.release()
                    }
                }
            }

            downloadJobs.joinAll()
            segmentAvailableChannel.trySend(Unit)
            writerJob.join()
            chatJob?.join()
        }
    }

    private suspend fun downloadPlaylist(
        offlineVideo: OfflineVideo,
        downloadProgress: DownloadProgress,
        networkLibrary: String?,
        urlPath: String,
        path: String,
        playlist: MediaPlaylist,
        segments: List<Segment>
    ) = withContext(Dispatchers.IO) {
        val videoDirectoryName = if (!offlineVideo.videoId.isNullOrBlank()) {
            "${offlineVideo.videoId}${offlineVideo.quality ?: ""}"
        } else {
            "${offlineVideo.downloadDate}"
        }
        val (videoDirectoryUri, videoDirDocId) = SafUtils.getOrCreateDirectory(contentResolver, path, videoDirectoryName)
        val playlistFileUri = if (!offlineVideo.url.isNullOrBlank() && SafUtils.fileExists(contentResolver, offlineVideo.url!!)) {
            offlineVideo.url!!
        } else {
            val fileName = "${offlineVideo.downloadDate}.m3u8"
            val pFileUri = SafUtils.getOrCreateChildDocument(contentResolver, path, videoDirDocId, fileName, "application/x-mpegURL")
            SafUtils.openOutputStream(contentResolver, pFileUri, append = false).use {
                PlaylistUtils.writeMediaPlaylist(playlist.copy(
                    initSegmentUri = playlist.initSegmentUri?.let { uri -> SafUtils.getOrCreateChildDocument(contentResolver, path, videoDirDocId, uri, "video/mp4") },
                    segments = segments.map { segment ->
                        val childUri = SafUtils.getOrCreateChildDocument(contentResolver, path, videoDirDocId, segment.uri, "video/mp2t")
                        segment.copy(uri = childUri)
                    }
                ), it)
            }
            if (playlist.initSegmentUri != null) {
                val initSegmentFileUri = SafUtils.getOrCreateChildDocument(contentResolver, path, videoDirDocId, playlist.initSegmentUri, "video/mp4")
                val initUrl = if (playlist.initSegmentUri.startsWith("http://") || playlist.initSegmentUri.startsWith("https://")) {
                    playlist.initSegmentUri
                } else {
                    urlPath + playlist.initSegmentUri
                }
                val initData = downloadByteArray(networkLibrary, initUrl)
                SafUtils.openOutputStream(contentResolver, initSegmentFileUri, append = false).use {
                    it.write(initData)
                }
            }
            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                url = pFileUri
            })
            pFileUri
        }
        val downloadedSegments = mutableListOf<String>()
        if (SafUtils.isContentUri(path)) {
            val playlists = xtraModule.offlineVideosRepository.getPlaylists().mapNotNull { video ->
                video.url?.takeIf {
                    SafUtils.isContentUri(it)
                            && it.substringBeforeLast("%2F") == videoDirectoryUri
                            && it != playlistFileUri
                }
            }
            playlists.forEach { uri ->
                try {
                    val p = SafUtils.openInputStream(contentResolver, uri).use {
                        PlaylistUtils.parseMediaPlaylist(it)
                    }
                    p.segments.forEach { downloadedSegments.add(it.uri.substringAfterLast("%2F").substringAfterLast("/")) }
                } catch (e: Exception) {

                }
            }
        } else {
            val playlists = File(videoDirectoryUri).listFiles { it.extension == "m3u8" && it.path != playlistFileUri }
            playlists?.forEach { file ->
                val p = PlaylistUtils.parseMediaPlaylist(file.inputStream())
                p.segments.forEach { downloadedSegments.add(it.uri.substringAfterLast("%2F").substringAfterLast("/")) }
            }
        }

        downloadProgress.lastSaved = System.currentTimeMillis()

        coroutineScope {
            var chatJob: Job? = null
            if (offlineVideo.downloadChat && downloadProgress.chatProgress < downloadProgress.maxChatProgress) {
                chatJob = launch(Dispatchers.IO) {
                    try {
                        startChatJob(offlineVideo, downloadProgress, path)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        Log.e("VideoDownloadService", "Chat download failed", e)
                    }
                }
            }

            val requestSemaphore = Semaphore(prefs().getInt(C.DOWNLOAD_CONCURRENT_LIMIT, 10))
            val progressMutex = Mutex()
            val downloadJobs = (downloadProgress.progress until segments.size).map { index ->
                val segment = segments[index]
                launch(Dispatchers.IO) {
                    requestSemaphore.acquire()
                    try {
                        val fileUri = SafUtils.getOrCreateChildDocument(contentResolver, path, videoDirDocId, segment.uri, "video/mp2t")
                        val exists = SafUtils.fileExists(contentResolver, fileUri)
                        if (!exists || !downloadedSegments.contains(segment.uri)) {
                            val segmentUrl = if (segment.uri.startsWith("http://") || segment.uri.startsWith("https://")) {
                                segment.uri
                            } else {
                                urlPath + segment.uri
                            }
                            var segmentBytes: ByteArray? = null
                            var segmentRetries = 3
                            while (segmentBytes == null && segmentRetries > 0) {
                                try {
                                    segmentBytes = downloadByteArray(networkLibrary, segmentUrl)
                                } catch (e: CancellationException) {
                                    throw e
                                } catch (e: Exception) {
                                    segmentRetries--
                                    if (segmentRetries > 0) {
                                        delay(1000L)
                                    } else {
                                        if (segmentUrl.contains("-muted")) {
                                            try {
                                                segmentBytes = downloadByteArray(networkLibrary, segmentUrl.replace("-muted", "-unmuted"))
                                            } catch (e2: Exception) {
                                                try {
                                                    segmentBytes = downloadByteArray(networkLibrary, segmentUrl.replace("-muted", ""))
                                                } catch (e3: Exception) {
                                                    throw e
                                                }
                                            }
                                        } else if (segmentUrl.contains("-unmuted")) {
                                            try {
                                                segmentBytes = downloadByteArray(networkLibrary, segmentUrl.replace("-unmuted", "-muted"))
                                            } catch (e2: Exception) {
                                                try {
                                                    segmentBytes = downloadByteArray(networkLibrary, segmentUrl.replace("-unmuted", ""))
                                                } catch (e3: Exception) {
                                                    throw e
                                                }
                                            }
                                        } else {
                                            throw e
                                        }
                                    }
                                }
                            }
                            if (segmentBytes != null) {
                                SafUtils.openOutputStream(contentResolver, fileUri, append = false).use {
                                    it.write(segmentBytes)
                                }
                            }
                        }
                        progressMutex.withLock {
                            downloadProgress.progress += 1
                            listener?.update(downloadProgress)
                            sendNotification(offlineVideo, downloadProgress)
                            val currentTime = System.currentTimeMillis()
                            if (currentTime - downloadProgress.lastSaved >= 5000L) {
                                downloadProgress.lastSaved = currentTime
                                xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                                    progress = downloadProgress.progress
                                    maxProgress = downloadProgress.maxProgress
                                    bytes = downloadProgress.bytes
                                    chatProgress = downloadProgress.chatProgress
                                    maxChatProgress = downloadProgress.maxChatProgress
                                    chatBytes = downloadProgress.chatBytes
                                    chatOffsetSeconds = downloadProgress.chatOffsetSeconds
                                })
                            }
                        }
                    } finally {
                        requestSemaphore.release()
                    }
                }
            }
            downloadJobs.joinAll()
            chatJob?.join()
        }
    }

    private suspend fun downloadClip(offlineVideo: OfflineVideo, downloadProgress: DownloadProgress, sourceUrl: String) = withContext(Dispatchers.IO) {
        val networkLibrary = prefs().getString(C.NETWORK_LIBRARY, C.OKHTTP)
        val path = offlineVideo.downloadPath!!
        val videoFileUri = if (!offlineVideo.url.isNullOrBlank() && SafUtils.fileExists(contentResolver, offlineVideo.url!!)) {
            offlineVideo.url!!
        } else {
            val fileName = if (!offlineVideo.clipId.isNullOrBlank()) {
                "${offlineVideo.clipId}${offlineVideo.quality ?: ""}.mp4"
            } else {
                "${offlineVideo.downloadDate}.mp4"
            }
            val fileUri = SafUtils.getOrCreateDocument(contentResolver, path, fileName, "video/mp4")
            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                url = fileUri
            })
            fileUri
        }
        downloadProgress.lastSaved = System.currentTimeMillis()

        coroutineScope {
            var chatJob: Job? = null
            if (offlineVideo.downloadChat && downloadProgress.chatProgress < downloadProgress.maxChatProgress) {
                chatJob = launch(Dispatchers.IO) {
                    try {
                        startChatJob(offlineVideo, downloadProgress, path)
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        Log.e("VideoDownloadService", "Chat download failed", e)
                    }
                }
            }

            if (downloadProgress.progress < downloadProgress.maxProgress) {
                SafUtils.openOutputStream(contentResolver, videoFileUri, append = false).use { out ->
                    var lastUpdate = System.currentTimeMillis()
                    downloadToStream(networkLibrary, sourceUrl, out) { bytesRead, totalBytes ->
                        downloadProgress.bytes = bytesRead
                        if (totalBytes > 0) {
                            downloadProgress.progress = ((bytesRead.toDouble() / totalBytes) * downloadProgress.maxProgress).toInt().coerceIn(0, downloadProgress.maxProgress)
                        }
                        val now = System.currentTimeMillis()
                        if (now - lastUpdate >= 1000L) {
                            lastUpdate = now
                            listener?.update(downloadProgress)
                            sendNotification(offlineVideo, downloadProgress)
                        }
                    }
                }
                downloadProgress.progress = downloadProgress.maxProgress
                listener?.update(downloadProgress)
                sendNotification(offlineVideo, downloadProgress)
            }
            chatJob?.join()
        }
    }

    private suspend fun startChatJob(offlineVideo: OfflineVideo, downloadProgress: DownloadProgress, path: String) = withContext(Dispatchers.IO) {
        val isChatOnly = offlineVideo.quality == "chat_only"
        if ((offlineVideo.downloadChat || isChatOnly) && downloadProgress.chatProgress < downloadProgress.maxChatProgress) {
            val videoId = offlineVideo.videoId
            if (videoId != null) {
                val startTimeSeconds = (offlineVideo.sourceStartPosition?.div(1000L) ?: 0L).toInt()
                val durationSeconds = (offlineVideo.duration?.div(1000L) ?: 0L).toInt().coerceAtLeast(1)
                val endTimeSeconds = startTimeSeconds + durationSeconds
                val resumed = !offlineVideo.chatUrl.isNullOrBlank() && downloadProgress.chatBytes > 0
                val fileUri = if (resumed && SafUtils.fileExists(contentResolver, offlineVideo.chatUrl!!)) {
                    offlineVideo.chatUrl!!
                } else {
                    val fileName = "${videoId}${offlineVideo.quality ?: ""}${offlineVideo.downloadDate}_chat.json"
                    val fileUri = SafUtils.getOrCreateDocument(contentResolver, path, fileName, "application/json")
                    xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                        maxChatProgress = durationSeconds
                        chatOffsetSeconds = startTimeSeconds
                        chatUrl = fileUri
                    })
                    downloadProgress.maxChatProgress = durationSeconds
                    downloadProgress.chatOffsetSeconds = startTimeSeconds
                    fileUri
                }
                if (isChatOnly) {
                    downloadProgress.maxProgress = durationSeconds
                    downloadProgress.progress = downloadProgress.chatProgress
                }
                val downloadEmotes = offlineVideo.downloadChatEmotes
                val networkLibrary = prefs().getString(C.NETWORK_LIBRARY, C.OKHTTP)
                val gqlHeaders = TwitchApiHelper.getGQLHeaders(this@VideoDownloadService, true)
                val helixHeaders = TwitchApiHelper.getHelixHeaders(this@VideoDownloadService)
                val emoteQuality = prefs().getString(C.CHAT_IMAGE_QUALITY, "4") ?: "4"
                val useWebp = prefs().getBoolean(C.CHAT_USE_WEBP, true)
                val channelId = offlineVideo.channelId
                val channelLogin = offlineVideo.channelLogin
                val globalBadgeList = mutableListOf<TwitchBadge>()
                val channelBadgeList = mutableListOf<TwitchBadge>()
                val cheerEmoteList = mutableListOf<CheerEmote>()
                val emoteList = mutableListOf<Emote>()
                if (downloadEmotes) {
                    val jobs = mutableListOf<Job>().apply {
                        add(launch(Dispatchers.IO) {
                            try {
                                val badges = xtraModule.playerRepository.loadGlobalBadges(networkLibrary, helixHeaders, gqlHeaders, emoteQuality, false)
                                globalBadgeList.addAll(badges)
                            } catch (e: Exception) {

                            }
                        })
                        add(launch(Dispatchers.IO) {
                            try {
                                val response = xtraModule.playerRepository.loadGlobalSTVEmoteSetResponse(networkLibrary)
                                val emotes = xtraModule.playerRepository.loadSTVEmoteSet(response, useWebp, true).second
                                emoteList.addAll(emotes)
                                emoteList.sortBy { it.source }
                            } catch (e: Exception) {

                            }
                        })
                        add(launch(Dispatchers.IO) {
                            try {
                                val response = xtraModule.playerRepository.loadGlobalBTTVEmotesResponse(networkLibrary)
                                val emotes = xtraModule.playerRepository.loadGlobalBTTVEmotes(response, useWebp)
                                emoteList.addAll(emotes)
                                emoteList.sortBy { it.source }
                            } catch (e: Exception) {

                            }
                        })
                        add(launch(Dispatchers.IO) {
                            try {
                                val response = xtraModule.playerRepository.loadGlobalFFZEmotesResponse(networkLibrary)
                                val emotes = xtraModule.playerRepository.loadGlobalFFZEmotes(response, useWebp)
                                emoteList.addAll(emotes)
                                emoteList.sortBy { it.source }
                            } catch (e: Exception) {

                            }
                        })
                        if (channelId != null) {
                            add(launch(Dispatchers.IO) {
                                try {
                                    val userResponse = xtraModule.playerRepository.loadSTVUserResponse(networkLibrary, channelId)
                                    val user = xtraModule.playerRepository.loadSTVUser(userResponse, useWebp)
                                    val userSetId = user.first
                                    val userEmotes = user.second
                                    val emotes = if (!userEmotes.isNullOrEmpty()) {
                                        userEmotes
                                    } else {
                                        if (!userSetId.isNullOrBlank()) {
                                            val emoteSetResponse = xtraModule.playerRepository.loadSTVEmoteSetResponse(networkLibrary, userSetId)
                                            val emoteSet = xtraModule.playerRepository.loadSTVEmoteSet(emoteSetResponse, useWebp, false)
                                            emoteSet.second
                                        } else emptyList()
                                    }
                                    emoteList.addAll(emotes)
                                    emoteList.sortBy { it.source }
                                } catch (e: Exception) {

                                }
                            })
                            add(launch(Dispatchers.IO) {
                                try {
                                    val response = xtraModule.playerRepository.loadBTTVEmotesResponse(networkLibrary, channelId)
                                    val emotes = xtraModule.playerRepository.loadBTTVEmotes(response, useWebp)
                                    emoteList.addAll(emotes)
                                    emoteList.sortBy { it.source }
                                } catch (e: Exception) {

                                }
                            })
                            add(launch(Dispatchers.IO) {
                                try {
                                    val response = xtraModule.playerRepository.loadFFZEmotesResponse(networkLibrary, channelId)
                                    val emotes = xtraModule.playerRepository.loadFFZEmotes(response, useWebp)
                                    emoteList.addAll(emotes)
                                    emoteList.sortBy { it.source }
                                } catch (e: Exception) {

                                }
                            })
                            add(launch(Dispatchers.IO) {
                                try {
                                    val badges = xtraModule.playerRepository.loadChannelBadges(networkLibrary, helixHeaders, gqlHeaders, channelId, channelLogin, emoteQuality, false)
                                    channelBadgeList.addAll(badges)
                                } catch (e: Exception) {

                                }
                            })
                            add(launch(Dispatchers.IO) {
                                try {
                                    val emotes = xtraModule.playerRepository.loadCheerEmotes(networkLibrary, helixHeaders, gqlHeaders, channelId, channelLogin, animateGifs = true, enableIntegrity = false)
                                    cheerEmoteList.addAll(emotes)
                                } catch (e: Exception) {

                                }
                            })
                        }
                    }
                    jobs.joinAll()
                }
                var position = downloadProgress.chatBytes
                val startOffset = downloadProgress.chatOffsetSeconds
                val latestSavedMessageIds = mutableListOf<String>()
                val savedTwitchEmotes = mutableListOf<String>()
                val savedBadges = mutableListOf<Pair<String, String>>()
                val savedEmotes = mutableListOf<String>()
                if (resumed) {
                    SafUtils.truncateFile(contentResolver, fileUri, downloadProgress.chatBytes)
                    SafUtils.openOutputStream(contentResolver, fileUri, append = true).bufferedWriter().use { writer ->
                        writer.write("}")
                    }
                    try {
                        SafUtils.openInputStream(contentResolver, fileUri).bufferedReader().use { fileReader ->
                            JsonReader(fileReader).use { reader ->
                                reader.isLenient = true
                                var token: JsonToken
                                do {
                                    token = reader.peek()
                                    when (token) {
                                        JsonToken.END_DOCUMENT -> {}
                                        JsonToken.BEGIN_OBJECT -> {
                                            reader.beginObject()
                                            while (reader.hasNext()) {
                                                when (reader.peek()) {
                                                    JsonToken.NAME -> {
                                                        when (reader.nextName()) {
                                                            "comments" -> {
                                                                reader.beginArray()
                                                                while (reader.hasNext()) {
                                                                    reader.beginObject()
                                                                    var id: String? = null
                                                                    while (reader.hasNext()) {
                                                                        when (reader.nextName()) {
                                                                            "id" -> id = reader.nextString()
                                                                            else -> reader.skipValue()
                                                                        }
                                                                    }
                                                                    if (!id.isNullOrBlank()) {
                                                                        latestSavedMessageIds.add(id)
                                                                    }
                                                                    reader.endObject()
                                                                }
                                                                reader.endArray()
                                                            }
                                                            "twitchEmotes" -> {
                                                                reader.beginArray()
                                                                while (reader.hasNext()) {
                                                                    reader.beginObject()
                                                                    var id: String? = null
                                                                    while (reader.hasNext()) {
                                                                        when (reader.nextName()) {
                                                                            "id" -> id = reader.nextString()
                                                                            else -> reader.skipValue()
                                                                        }
                                                                    }
                                                                    if (!id.isNullOrBlank()) {
                                                                        savedTwitchEmotes.add(id)
                                                                    }
                                                                    reader.endObject()
                                                                }
                                                                reader.endArray()
                                                            }
                                                            "twitchBadges" -> {
                                                                reader.beginArray()
                                                                while (reader.hasNext()) {
                                                                    reader.beginObject()
                                                                    var setId: String? = null
                                                                    var version: String? = null
                                                                    while (reader.hasNext()) {
                                                                        when (reader.nextName()) {
                                                                            "setId" -> setId = reader.nextString()
                                                                            "version" -> version = reader.nextString()
                                                                            else -> reader.skipValue()
                                                                        }
                                                                    }
                                                                    if (!setId.isNullOrBlank() && !version.isNullOrBlank()) {
                                                                        savedBadges.add(Pair(setId, version))
                                                                    }
                                                                    reader.endObject()
                                                                }
                                                                reader.endArray()
                                                            }
                                                            "cheerEmotes" -> {
                                                                reader.beginArray()
                                                                while (reader.hasNext()) {
                                                                    reader.beginObject()
                                                                    var name: String? = null
                                                                    while (reader.hasNext()) {
                                                                        when (reader.nextName()) {
                                                                            "name" -> name = reader.nextString()
                                                                            else -> reader.skipValue()
                                                                        }
                                                                    }
                                                                    if (!name.isNullOrBlank()) {
                                                                        savedEmotes.add(name)
                                                                    }
                                                                    reader.endObject()
                                                                }
                                                                reader.endArray()
                                                            }
                                                            "emotes" -> {
                                                                reader.beginArray()
                                                                while (reader.hasNext()) {
                                                                    reader.beginObject()
                                                                    var name: String? = null
                                                                    while (reader.hasNext()) {
                                                                        when (reader.nextName()) {
                                                                            "name" -> name = reader.nextString()
                                                                            else -> reader.skipValue()
                                                                        }
                                                                    }
                                                                    if (!name.isNullOrBlank()) {
                                                                        savedEmotes.add(name)
                                                                    }
                                                                    reader.endObject()
                                                                }
                                                                reader.endArray()
                                                            }
                                                            else -> reader.skipValue()
                                                        }
                                                    }
                                                    else -> reader.skipValue()
                                                }
                                            }
                                            reader.endObject()
                                        }
                                        else -> reader.skipValue()
                                    }
                                } while (token != JsonToken.END_DOCUMENT)
                            }
                        }
                    } catch (e: Exception) {
                        Log.w("VideoDownloadService", "Error parsing existing chat json", e)
                    }
                    SafUtils.truncateFile(contentResolver, fileUri, downloadProgress.chatBytes)
                } else {
                    SafUtils.openOutputStream(contentResolver, fileUri, append = false).bufferedWriter().use { writer ->
                        writer.write("{".also { position += 1 })
                        writer.write("\"video\":".also { position += it.length })
                        writer.write(
                            buildJsonObject {
                                put("id", videoId)
                                offlineVideo.name?.let { put("title", it) }
                                offlineVideo.uploadDate?.let { put("uploadDate", it) }
                                offlineVideo.channelId?.let { put("channelId", it) }
                                offlineVideo.channelLogin?.let { put("channelLogin", it) }
                                offlineVideo.channelName?.let { put("channelName", it) }
                                offlineVideo.gameId?.let { put("gameId", it) }
                                offlineVideo.gameSlug?.let { put("gameSlug", it) }
                                offlineVideo.gameName?.let { put("gameName", it) }
                            }.toString().also { position += it.toByteArray().size }
                        )
                        writer.write(",".also { position += 1 })
                        writer.write("\"startTime\":$startTimeSeconds".also { position += it.length })
                    }
                }
                var cursor: String? = null
                while (true) {
                    val response = try {
                        if (cursor == null) {
                            xtraModule.graphQLRepository.loadQueryVideoCommentsDownload(networkLibrary, CRONET_TIMEOUT, okHttpClient, gqlHeaders, videoId, offset = startOffset)
                        } else {
                            xtraModule.graphQLRepository.loadQueryVideoCommentsDownload(networkLibrary, CRONET_TIMEOUT, okHttpClient, gqlHeaders, videoId, cursor = cursor)
                        }
                    } catch (e: Exception) {
                        Log.e("VideoDownloadService", "Error loading video comments", e)
                        null
                    }
                    val comments = response?.data?.video?.comments
                    if (comments == null) {
                        SafUtils.openOutputStream(contentResolver, fileUri, append = true).bufferedWriter().use { writer ->
                            writer.write("}".also { position += 1 })
                        }
                        downloadProgress.chatProgress = downloadProgress.maxChatProgress
                        downloadProgress.chatBytes = position
                        if (isChatOnly) {
                            downloadProgress.progress = downloadProgress.maxProgress
                            downloadProgress.bytes = position
                        }
                        listener?.update(downloadProgress)
                        break
                    }
                    val messages = if (cursor == null && resumed) {
                        comments.edges.filter { item ->
                            val id = item.node.id
                            val offset = item.node.contentOffsetSeconds
                            id != null && offset != null && ((offset == startOffset && !latestSavedMessageIds.contains(id)) || offset > startOffset)
                        }
                    } else {
                        comments.edges
                    }
                    cursor = if (comments.pageInfo?.hasNextPage != false) comments.edges.lastOrNull()?.cursor else null
                    if (messages.isNotEmpty()) {
                        SafUtils.openOutputStream(contentResolver, fileUri, append = true).bufferedWriter().use { writer ->
                            writer.write(",".also { position += 1 })
                            writer.write("\"comments\":".also { position += it.length })
                            writer.write("[".also { position += 1 })
                            messages.forEachIndexed { index, message ->
                                if (index > 0) {
                                    writer.write(",".also { position += 1 })
                                }
                                writer.write(xtraModule.json.encodeToString(message.node).also { position += it.toByteArray().size })
                            }
                            writer.write("]".also { position += 1 })
                        }
                        if (downloadEmotes) {
                            val twitchEmotes = mutableListOf<TwitchEmote>()
                            val twitchBadges = mutableListOf<TwitchBadge>()
                            val cheerEmotes = mutableListOf<CheerEmote>()
                            val emotes = mutableListOf<Emote>()
                            val words = mutableListOf<String>()
                            messages.forEach { comment ->
                                comment.node.let { item ->
                                    item.message?.let { message ->
                                        val chatMessage = StringBuilder()
                                        message.fragments?.forEach { fragment ->
                                            fragment.text?.let { text ->
                                                fragment.emote?.emoteID?.let { id ->
                                                    if (!savedTwitchEmotes.contains(id)) {
                                                        savedTwitchEmotes.add(id)
                                                        twitchEmotes.add(TwitchEmote(id = id))
                                                    }
                                                }
                                                chatMessage.append(text)
                                            }
                                        }
                                        message.userBadges?.forEach { badge ->
                                            badge.setID?.let { setId ->
                                                badge.version?.let { version ->
                                                    val pair = Pair(setId, version)
                                                    if (!savedBadges.contains(pair)) {
                                                        savedBadges.add(pair)
                                                        val badgeObj = channelBadgeList.find { b -> b.setId == setId && b.version == version }
                                                            ?: globalBadgeList.find { b -> b.setId == setId && b.version == version }
                                                        if (badgeObj != null) {
                                                            twitchBadges.add(badgeObj)
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        chatMessage.toString().split(" ").forEach { string ->
                                            if (!words.contains(string)) {
                                                words.add(string)
                                                if (!savedEmotes.contains(string)) {
                                                    val bitsCount = string.takeLastWhile { it.isDigit() }
                                                    val cheerEmote = if (bitsCount.isNotEmpty()) {
                                                        val bitsName = string.substringBeforeLast(bitsCount)
                                                        cheerEmoteList.findLast { it.name.equals(bitsName, true) && it.minBits <= bitsCount.toInt() }
                                                    } else null
                                                    if (cheerEmote != null) {
                                                        savedEmotes.add(string)
                                                        cheerEmotes.add(cheerEmote)
                                                    } else {
                                                        val emote = emoteList.find { it.name == string }
                                                        if (emote != null) {
                                                            savedEmotes.add(string)
                                                            emotes.add(emote)
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            coroutineScope {
                                val emoteSemaphore = Semaphore(10)
                                if (twitchEmotes.isNotEmpty()) {
                                    val downloaded = twitchEmotes.map { emote ->
                                        async(Dispatchers.IO) {
                                            emoteSemaphore.acquire()
                                            try {
                                                val url = when (emoteQuality) {
                                                    "4" -> emote.url4x ?: emote.url3x ?: emote.url2x ?: emote.url1x
                                                    "3" -> emote.url3x ?: emote.url2x ?: emote.url1x
                                                    "2" -> emote.url2x ?: emote.url1x
                                                    else -> emote.url1x
                                                } ?: return@async null
                                                val response = try {
                                                    downloadByteArray(networkLibrary, url)
                                                } catch (e: Exception) {
                                                    null
                                                }
                                                if (response != null) {
                                                    buildJsonObject {
                                                        put("data", Base64.encodeToString(response, Base64.NO_WRAP or Base64.NO_PADDING))
                                                        put("id", emote.id)
                                                    }
                                                } else null
                                            } finally {
                                                emoteSemaphore.release()
                                            }
                                        }
                                    }.awaitAll().filterNotNull()

                                    if (downloaded.isNotEmpty()) {
                                        SafUtils.openOutputStream(contentResolver, fileUri, append = true).bufferedWriter().use { writer ->
                                            writer.write(",\"twitchEmotes\":[".also { position += it.length })
                                            downloaded.forEachIndexed { index, item ->
                                                if (index > 0) writer.write(",".also { position += 1 })
                                                val str = item.toString()
                                                writer.write(str.also { position += it.toByteArray().size })
                                            }
                                            writer.write("]".also { position += 1 })
                                        }
                                    }
                                }

                                if (twitchBadges.isNotEmpty()) {
                                    val downloaded = twitchBadges.map { badge ->
                                        async(Dispatchers.IO) {
                                            emoteSemaphore.acquire()
                                            try {
                                                val url = when (emoteQuality) {
                                                    "4" -> badge.url4x ?: badge.url3x ?: badge.url2x ?: badge.url1x
                                                    "3" -> badge.url3x ?: badge.url2x ?: badge.url1x
                                                    "2" -> badge.url2x ?: badge.url1x
                                                    else -> badge.url1x
                                                } ?: return@async null
                                                val response = try {
                                                    downloadByteArray(networkLibrary, url)
                                                } catch (e: Exception) {
                                                    null
                                                }
                                                if (response != null) {
                                                    buildJsonObject {
                                                        put("data", Base64.encodeToString(response, Base64.NO_WRAP or Base64.NO_PADDING))
                                                        put("setId", badge.setId)
                                                        put("version", badge.version)
                                                    }
                                                } else null
                                            } finally {
                                                emoteSemaphore.release()
                                            }
                                        }
                                    }.awaitAll().filterNotNull()

                                    if (downloaded.isNotEmpty()) {
                                        SafUtils.openOutputStream(contentResolver, fileUri, append = true).bufferedWriter().use { writer ->
                                            writer.write(",\"twitchBadges\":[".also { position += it.length })
                                            downloaded.forEachIndexed { index, item ->
                                                if (index > 0) writer.write(",".also { position += 1 })
                                                val str = item.toString()
                                                writer.write(str.also { position += it.toByteArray().size })
                                            }
                                            writer.write("]".also { position += 1 })
                                        }
                                    }
                                }

                                if (cheerEmotes.isNotEmpty()) {
                                    val downloaded = cheerEmotes.map { cheerEmote ->
                                        async(Dispatchers.IO) {
                                            emoteSemaphore.acquire()
                                            try {
                                                val url = when (emoteQuality) {
                                                    "4" -> cheerEmote.url4x ?: cheerEmote.url3x ?: cheerEmote.url2x ?: cheerEmote.url1x
                                                    "3" -> cheerEmote.url3x ?: cheerEmote.url2x ?: cheerEmote.url1x
                                                    "2" -> cheerEmote.url2x ?: cheerEmote.url1x
                                                    else -> cheerEmote.url1x
                                                } ?: return@async null
                                                val response = try {
                                                    downloadByteArray(networkLibrary, url)
                                                } catch (e: Exception) {
                                                    null
                                                }
                                                if (response != null) {
                                                    buildJsonObject {
                                                        put("data", Base64.encodeToString(response, Base64.NO_WRAP or Base64.NO_PADDING))
                                                        put("name", cheerEmote.name)
                                                        put("minBits", cheerEmote.minBits)
                                                        cheerEmote.color?.let { put("color", it) }
                                                    }
                                                } else null
                                            } finally {
                                                emoteSemaphore.release()
                                            }
                                        }
                                    }.awaitAll().filterNotNull()

                                    if (downloaded.isNotEmpty()) {
                                        SafUtils.openOutputStream(contentResolver, fileUri, append = true).bufferedWriter().use { writer ->
                                            writer.write(",\"cheerEmotes\":[".also { position += it.length })
                                            downloaded.forEachIndexed { index, item ->
                                                if (index > 0) writer.write(",".also { position += 1 })
                                                val str = item.toString()
                                                writer.write(str.also { position += it.toByteArray().size })
                                            }
                                            writer.write("]".also { position += 1 })
                                        }
                                    }
                                }

                                if (emotes.isNotEmpty()) {
                                    val downloaded = emotes.map { emote ->
                                        async(Dispatchers.IO) {
                                            emoteSemaphore.acquire()
                                            try {
                                                val url = when (emoteQuality) {
                                                    "4" -> emote.url4x ?: emote.url3x ?: emote.url2x ?: emote.url1x
                                                    "3" -> emote.url3x ?: emote.url2x ?: emote.url1x
                                                    "2" -> emote.url2x ?: emote.url1x
                                                    else -> emote.url1x
                                                } ?: return@async null
                                                val response = try {
                                                    downloadByteArray(networkLibrary, url)
                                                } catch (e: Exception) {
                                                    null
                                                }
                                                if (response != null) {
                                                    buildJsonObject {
                                                        put("data", Base64.encodeToString(response, Base64.NO_WRAP or Base64.NO_PADDING))
                                                        put("name", emote.name)
                                                        put("isZeroWidth", emote.isOverlayEmote)
                                                    }
                                                } else null
                                            } finally {
                                                emoteSemaphore.release()
                                            }
                                        }
                                    }.awaitAll().filterNotNull()

                                    if (downloaded.isNotEmpty()) {
                                        SafUtils.openOutputStream(contentResolver, fileUri, append = true).bufferedWriter().use { writer ->
                                            writer.write(",\"emotes\":[".also { position += it.length })
                                            downloaded.forEachIndexed { index, item ->
                                                if (index > 0) writer.write(",".also { position += 1 })
                                                val str = item.toString()
                                                writer.write(str.also { position += it.toByteArray().size })
                                            }
                                            writer.write("]".also { position += 1 })
                                        }
                                    }
                                }
                            }
                        }
                    }
                    val lastOffsetSeconds = comments.edges.lastOrNull()?.node?.contentOffsetSeconds
                    if (lastOffsetSeconds != null && lastOffsetSeconds < endTimeSeconds && !cursor.isNullOrBlank()) {
                        downloadProgress.chatProgress = lastOffsetSeconds - startTimeSeconds
                        downloadProgress.chatBytes = position
                        downloadProgress.chatOffsetSeconds = lastOffsetSeconds
                        if (isChatOnly) {
                            downloadProgress.progress = downloadProgress.chatProgress
                            downloadProgress.bytes = position
                        }
                        listener?.update(downloadProgress)
                        sendNotification(offlineVideo, downloadProgress)
                        val currentTime = System.currentTimeMillis()
                        if (currentTime - downloadProgress.lastSaved >= 5000L) {
                            downloadProgress.lastSaved = currentTime
                            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                                progress = downloadProgress.progress
                                maxProgress = downloadProgress.maxProgress
                                bytes = downloadProgress.bytes
                                chatProgress = downloadProgress.chatProgress
                                maxChatProgress = downloadProgress.maxChatProgress
                                chatBytes = downloadProgress.chatBytes
                                chatOffsetSeconds = downloadProgress.chatOffsetSeconds
                            })
                        }
                    } else {
                        SafUtils.openOutputStream(contentResolver, fileUri, append = true).bufferedWriter().use { writer ->
                            writer.write("}".also { position += 1 })
                        }
                        downloadProgress.chatProgress = downloadProgress.maxChatProgress
                        downloadProgress.chatBytes = position
                        if (lastOffsetSeconds != null) {
                            downloadProgress.chatOffsetSeconds = lastOffsetSeconds
                        }
                        if (isChatOnly) {
                            downloadProgress.progress = downloadProgress.maxProgress
                            downloadProgress.bytes = position
                        }
                        listener?.update(downloadProgress)
                        sendNotification(offlineVideo, downloadProgress)
                        break
                    }
                }
            } else {
                downloadProgress.chatProgress = downloadProgress.maxChatProgress
                if (isChatOnly) {
                    downloadProgress.progress = downloadProgress.maxProgress
                }
                listener?.update(downloadProgress)
                sendNotification(offlineVideo, downloadProgress)
            }
        }
    }

    private fun sendNotification(offlineVideo: OfflineVideo, downloadProgress: DownloadProgress, paused: Boolean = false) {
        val isChatOnly = offlineVideo.quality == "chat_only"
        val notification = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            Notification.Builder(this, getString(R.string.notification_downloads_channel_id))
        } else {
            @Suppress("DEPRECATION")
            Notification.Builder(this)
        }.apply {
            setContentTitle(ContextCompat.getString(this@VideoDownloadService, R.string.downloading))
            setContentText(offlineVideo.name)
            setSmallIcon(android.R.drawable.stat_sys_download)
            setGroup(GROUP_KEY)
            setOngoing(true)
            setOnlyAlertOnce(true)
            if (isChatOnly) {
                setProgress(downloadProgress.maxChatProgress, downloadProgress.chatProgress, false)
            } else {
                setProgress(downloadProgress.maxProgress, downloadProgress.progress, false)
            }
            setContentIntent(
                PendingIntent.getActivity(
                    this@VideoDownloadService,
                    offlineVideo.id,
                    Intent(this@VideoDownloadService, MainActivity::class.java).apply {
                        flags = Intent.FLAG_ACTIVITY_CLEAR_TOP
                        action = MainActivity.INTENT_OPEN_DOWNLOADS_TAB
                    },
                    PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
                )
            )
            if (paused) {
                addAction(
                    Notification.Action.Builder(
                        Icon.createWithResource(this@VideoDownloadService, R.drawable.baseline_play_arrow_black_48),
                        ContextCompat.getString(this@VideoDownloadService, R.string.resume),
                        PendingIntent.getService(
                            this@VideoDownloadService,
                            REQUEST_CODE_RESUME,
                            Intent(this@VideoDownloadService, VideoDownloadService::class.java).apply {
                                action = INTENT_RESUME
                                putExtra(KEY_VIDEO_ID, offlineVideo.id)
                            },
                            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
                        )
                    ).build()
                )
            } else {
                addAction(
                    Notification.Action.Builder(
                        Icon.createWithResource(this@VideoDownloadService, R.drawable.baseline_pause_black_48),
                        ContextCompat.getString(this@VideoDownloadService, R.string.pause),
                        PendingIntent.getService(
                            this@VideoDownloadService,
                            REQUEST_CODE_PAUSE,
                            Intent(this@VideoDownloadService, VideoDownloadService::class.java).apply {
                                action = INTENT_PAUSE
                                putExtra(KEY_VIDEO_ID, offlineVideo.id)
                            },
                            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
                        )
                    ).build()
                )
            }
            addAction(
                Notification.Action.Builder(
                    Icon.createWithResource(this@VideoDownloadService, android.R.drawable.ic_delete),
                    ContextCompat.getString(this@VideoDownloadService, R.string.stop),
                    PendingIntent.getService(
                        this@VideoDownloadService,
                        REQUEST_CODE_STOP,
                        Intent(this@VideoDownloadService, VideoDownloadService::class.java).apply {
                            action = INTENT_STOP
                            putExtra(KEY_VIDEO_ID, offlineVideo.id)
                        },
                        PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
                    )
                ).build()
            )
        }.build()
        if (downloadProgress == activeDownloads.firstOrNull()) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
                startForeground(offlineVideo.id, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE)
            } else {
                startForeground(offlineVideo.id, notification)
            }
        } else {
            notificationManager?.notify(offlineVideo.id, notification)
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        super.onStartCommand(intent, flags, startId)
        when (intent?.action) {
            INTENT_PAUSE, INTENT_STOP, INTENT_CANCEL -> {
                val videoId = intent.getIntExtra(KEY_VIDEO_ID, 0)
                downloadJobs[videoId]?.cancel()
                val offlineVideo = offlineVideos.find { it.id == videoId }
                val downloadProgress = activeDownloads.find { it.id == videoId }
                if (offlineVideo != null && downloadProgress != null) {
                    offlineVideos.remove(offlineVideo)
                    activeDownloads.remove(downloadProgress)
                    if (intent.action != INTENT_CANCEL) {
                        if (intent.action == INTENT_PAUSE) {
                            sendNotification(offlineVideo, downloadProgress, paused = true)
                        }
                        lifecycleScope.launch(Dispatchers.IO) {
                            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                                status = OfflineVideo.STATUS_PENDING
                                progress = downloadProgress.progress
                                maxProgress = downloadProgress.maxProgress
                                bytes = downloadProgress.bytes
                                chatProgress = downloadProgress.chatProgress
                                maxChatProgress = downloadProgress.maxChatProgress
                                chatBytes = downloadProgress.chatBytes
                                chatOffsetSeconds = downloadProgress.chatOffsetSeconds
                            })
                        }
                    }
                }
                if (intent.action != INTENT_PAUSE) {
                    val nextOfflineVideo = offlineVideos.firstOrNull()
                    val nextDownload = activeDownloads.firstOrNull()
                    if (nextOfflineVideo != null && nextDownload != null) {
                        sendNotification(nextOfflineVideo, nextDownload)
                        notificationManager?.cancel(videoId)
                    } else {
                        listener?.unbind()
                        stopSelf()
                    }
                }
            }
            INTENT_START, INTENT_RESUME -> start(intent.getIntExtra(KEY_VIDEO_ID, 0))
        }
        return START_STICKY
    }

    override fun onBind(intent: Intent): IBinder {
        super.onBind(intent)
        return ServiceBinder()
    }

    inner class ServiceBinder : Binder() {
        fun getService() = this@VideoDownloadService
    }

    override fun onDestroy() {
        super.onDestroy()
        activeDownloads.forEach {
            notificationManager?.cancel(it.id)
        }
    }

    companion object {
        private const val CRONET_TIMEOUT = 300_000L
        private const val GROUP_KEY = "com.github.andreyasadchy.xtra.DOWNLOADS"

        private const val REQUEST_CODE_PAUSE = 0
        private const val REQUEST_CODE_RESUME = 1
        private const val REQUEST_CODE_STOP = 2

        const val KEY_VIDEO_ID = "videoId"

        private const val INTENT_PAUSE = "com.github.andreyasadchy.xtra.PAUSE"
        private const val INTENT_RESUME = "com.github.andreyasadchy.xtra.RESUME"
        const val INTENT_STOP = "com.github.andreyasadchy.xtra.STOP"
        const val INTENT_CANCEL = "com.github.andreyasadchy.xtra.CANCEL"
        const val INTENT_START = "com.github.andreyasadchy.xtra.START_VIDEO_DOWNLOAD_SERVICE"
    }
}