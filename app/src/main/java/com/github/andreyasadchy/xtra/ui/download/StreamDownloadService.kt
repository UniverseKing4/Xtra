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
import android.net.http.HttpEngine
import android.net.http.ProxyOptions
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
import com.github.andreyasadchy.xtra.model.VideoQuality
import com.github.andreyasadchy.xtra.model.chat.CheerEmote
import com.github.andreyasadchy.xtra.model.chat.Emote
import com.github.andreyasadchy.xtra.model.chat.TwitchBadge
import com.github.andreyasadchy.xtra.model.chat.TwitchEmote
import com.github.andreyasadchy.xtra.model.ui.DownloadProgress
import com.github.andreyasadchy.xtra.model.ui.OfflineVideo
import com.github.andreyasadchy.xtra.model.ui.Stream
import com.github.andreyasadchy.xtra.ui.main.MainActivity
import com.github.andreyasadchy.xtra.util.C
import com.github.andreyasadchy.xtra.util.NetworkUtils
import com.github.andreyasadchy.xtra.util.NetworkUtils.executeAsync
import com.github.andreyasadchy.xtra.util.TwitchApiHelper
import com.github.andreyasadchy.xtra.util.chat.ChatReadWebSocket
import com.github.andreyasadchy.xtra.util.chat.ChatUtils
import com.github.andreyasadchy.xtra.util.m3u8.PlaylistUtils
import com.github.andreyasadchy.xtra.util.SafUtils
import com.github.andreyasadchy.xtra.util.prefs
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.MainScope
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
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import okhttp3.Credentials
import okhttp3.Request
import org.chromium.net.CronetEngine
import org.chromium.net.CronetProvider
import org.chromium.net.QuicOptions
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.io.RandomAccessFile
import java.net.InetSocketAddress
import java.net.Proxy
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.coroutines.cancellation.CancellationException
import kotlin.math.floor
import kotlin.math.max
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class StreamDownloadService : LifecycleService() {

    lateinit var xtraModule: XtraModule
    private val okHttpClient = lazy {
        xtraModule.okHttpClient.value.newBuilder().apply {
            connectTimeout(5, TimeUnit.MINUTES)
            writeTimeout(5, TimeUnit.MINUTES)
            readTimeout(5, TimeUnit.MINUTES)
        }.build()
    }

    private var notificationManager: NotificationManager? = null
    private val downloadJobs = mutableListOf<DownloadJob>()
    private val offlineVideos = mutableListOf<OfflineVideo>()
    private val stoppedVideoIds = java.util.Collections.synchronizedSet(mutableSetOf<Int>())
    val activeDownloads = mutableListOf<DownloadProgress>()
    var listener: Listener? = null

    class DownloadJob(
        var id: Int,
        var job: Job? = null,
        var chatReadWebSocket: ChatReadWebSocket? = null,
    )

    interface Listener {
        fun unbind()
    }

    override fun onCreate() {
        super.onCreate()
        xtraModule = (application as XtraApp).xtraModule
    }

    private fun start(videoId: Int) {
        if (activeDownloads.find { it.id == videoId } == null) {
            val downloadJob = DownloadJob(videoId)
            lifecycleScope.launch(Dispatchers.IO) {
                val offlineVideo = xtraModule.offlineVideosRepository.getById(videoId)
                if (offlineVideo != null) {
                    val downloadProgress = DownloadProgress(
                        id = videoId,
                        bytes = offlineVideo.bytes,
                        chatBytes = offlineVideo.chatBytes,
                        lastSegmentUrl = offlineVideo.lastSegmentUrl,
                        liveCommentsArrayStarted = offlineVideo.liveCommentsArrayStarted,
                    )
                    offlineVideos.add(offlineVideo)
                    activeDownloads.add(downloadProgress)
                    xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                        status = OfflineVideo.STATUS_WAITING_FOR_STREAM
                    })
                    notificationManager = getSystemService(NOTIFICATION_SERVICE) as NotificationManager
                    val channelId = getString(R.string.notification_downloads_channel_id)
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O && notificationManager?.getNotificationChannel(channelId) == null) {
                        notificationManager?.createNotificationChannel(
                            NotificationChannel(
                                channelId,
                                ContextCompat.getString(this@StreamDownloadService, R.string.notification_downloads_channel_title),
                                NotificationManager.IMPORTANCE_DEFAULT
                            ).apply {
                                setSound(null, null)
                            }
                        )
                    }
                    sendNotification(offlineVideo, downloadProgress)
                    var retriesLeft = prefs().getString(C.DOWNLOAD_AUTO_RETRY_COUNT, "3")?.toIntOrNull() ?: 3
                    val autoRetry = prefs().getBoolean(C.DOWNLOAD_AUTO_RETRY, true)
                    var done = false
                    while (true) {
                        try {
                            val channelLogin = offlineVideo.channelLogin!!
                            downloadStream(offlineVideo, downloadProgress, downloadJob, channelLogin)
                            done = true
                            break
                        } catch (e: CancellationException) {
                            ensureActive()
                            break
                        } catch (e: Exception) {
                            Log.e("StreamDownloadService", "Download failed", e)
                            if (autoRetry && retriesLeft > 0) {
                                retriesLeft--
                                delay(5000L)
                            } else {
                                break
                            }
                        }
                    }
                    val wasStoppedByUser = stoppedVideoIds.remove(videoId)
                    offlineVideos.remove(offlineVideo)
                    activeDownloads.remove(downloadProgress)
                    xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                        status = if (wasStoppedByUser) {
                            OfflineVideo.STATUS_PENDING
                        } else if (done) {
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
                                OfflineVideo.STATUS_WAITING_FOR_NETWORK
                            }
                        }
                        if (!wasStoppedByUser && status != OfflineVideo.STATUS_DOWNLOADED && prefs().getBoolean(C.DOWNLOAD_AUTO_RETRY, true)) {
                            DownloadRetryWorker.enqueueRetry(applicationContext, 15L)
                        }
                        bytes = downloadProgress.bytes
                        chatBytes = downloadProgress.chatBytes
                        lastSegmentUrl = downloadProgress.lastSegmentUrl
                        liveCommentsArrayStarted = downloadProgress.liveCommentsArrayStarted
                    })
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
            }.also {
                it.invokeOnCompletion {
                    downloadJobs.remove(downloadJob)
                }
                downloadJob.job = it
                downloadJobs.add(downloadJob)
            }
        }
    }

    private suspend fun downloadStream(currentOfflineVideo: OfflineVideo, currentDownloadProgress: DownloadProgress, downloadJob: DownloadJob, channelLogin: String) = withContext(Dispatchers.IO) {
        val offlineCheck = max(prefs().getString(C.DOWNLOAD_STREAM_OFFLINE_CHECK, "10")?.toLongOrNull() ?: 10L, 2L) * 1000L
        val startWait = (prefs().getString(C.DOWNLOAD_STREAM_START_WAIT, "120")?.toLongOrNull())?.times(60000L)
        val endWait = (prefs().getString(C.DOWNLOAD_STREAM_END_WAIT, "15")?.toLongOrNull())?.times(60000L)
        val networkLibrary = prefs().getString(C.NETWORK_LIBRARY, C.OKHTTP)
        val gqlHeaders = TwitchApiHelper.getGQLHeaders(this@StreamDownloadService, prefs().getBoolean(C.TOKEN_INCLUDE_TOKEN_STREAM, true))
        val randomDeviceId = prefs().getBoolean(C.TOKEN_RANDOM_DEVICE_ID, true)
        val xDeviceId = prefs().getString(C.TOKEN_X_DEVICE_ID, "twitch-web-wall-mason")
        val playerType = prefs().getString(C.TOKEN_PLAYER_TYPE, "site")
        val supportedCodecs = prefs().getString(C.TOKEN_SUPPORTED_CODECS, "av1,h265,h264")
        val useCustomProxy = prefs().getBoolean(C.PLAYER_USE_CUSTOM_PROXY, true)
        val customProxyList = if (useCustomProxy) {
            xtraModule.playerRepository.getCustomProxies().filter {
                it.enabled && !it.url.isNullOrBlank()
            }.sortedBy { it.position }
        } else null
        var currentCustomProxy = 0
        val useStreamProxy = prefs().getBoolean(C.PLAYER_USE_STREAM_PROXY, false)
        val streamProxyList = if (useStreamProxy) {
            xtraModule.playerRepository.getStreamProxies().filter {
                it.enabled && !it.host.isNullOrBlank() && it.port != null
                        && (it.proxyPlaybackAccessToken || it.proxyMultivariantPlaylist)
            }.sortedBy { it.position }
        } else null
        var currentStreamProxy = 0
        var offlineVideo = currentOfflineVideo
        var downloadProgress = currentDownloadProgress
        val path = offlineVideo.downloadPath!!
        val quality = offlineVideo.quality
        var startTime = System.currentTimeMillis()
        var endTime = startWait?.let { System.currentTimeMillis() + it }
        var playlistUrl = xtraModule.playerRepository.loadStreamPlaylistUrl(this@StreamDownloadService, networkLibrary, gqlHeaders, channelLogin, randomDeviceId, xDeviceId, playerType, supportedCodecs, false, null, null, null, null, false)
        while (true) {
            val playlist = when {
                networkLibrary == C.HTTP_ENGINE && xtraModule.httpEngine.value != null -> @SuppressLint("NewApi") {
                    val response = suspendCancellableCoroutine { continuation ->
                        val timeout = NetworkUtils.HttpEngineTimeout(CRONET_TIMEOUT)
                        val request = xtraModule.httpEngine.value!!.newUrlRequestBuilder(
                            playlistUrl,
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
                        response.body.decodeToString()
                    } else null
                }
                networkLibrary == C.CRONET && xtraModule.cronetEngine.value != null -> {
                    val response = suspendCancellableCoroutine { continuation ->
                        val timeout = NetworkUtils.CronetTimeout(CRONET_TIMEOUT)
                        val request = xtraModule.cronetEngine.value!!.newUrlRequestBuilder(
                            playlistUrl,
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
                        response.body.decodeToString()
                    } else null
                }
                else -> {
                    okHttpClient.value.newCall(Request.Builder().url(playlistUrl).build()).executeAsync().use { response ->
                        if (response.isSuccessful) {
                            response.body.string()
                        } else null
                    }
                }
            }
            if (!playlist.isNullOrBlank()) {
                var proxyUrl = if (useCustomProxy) {
                    customProxyList?.getOrNull(currentCustomProxy)?.let { proxy ->
                        proxy.url?.let { proxyUrl ->
                            (proxyUrl.toUri().takeIf { it.host != null } ?: "https://$proxyUrl".toUri()).let { uri ->
                                if (proxy.addQueryParams) {
                                    val source = uri.getQueryParameter("allow_source") == null
                                    val audio = uri.getQueryParameter("allow_audio_only") == null
                                    val lowLatency = uri.getQueryParameter("fast_bread") == null
                                    if (source || audio || lowLatency) {
                                        uri.buildUpon().apply {
                                            if (source) {
                                                appendQueryParameter("allow_source", "true")
                                            }
                                            if (audio) {
                                                appendQueryParameter("allow_audio_only", "true")
                                            }
                                            if (lowLatency) {
                                                appendQueryParameter("fast_bread", "true")
                                            }
                                        }.build()
                                    } else uri
                                } else uri
                            }.toString().replace("\$channel", channelLogin)
                        }
                    }
                } else null
                val qualities = if (proxyUrl != null) {
                    var result: List<VideoQuality>
                    while (true) {
                        val newPlaylist = loadPlaylist(proxyUrl!!, networkLibrary, false, null, null, null, null)
                        if (!newPlaylist.isNullOrBlank()) {
                            result = getQualities(newPlaylist).ifEmpty { getQualities(playlist) }
                            break
                        } else {
                            currentCustomProxy += 1
                            proxyUrl = customProxyList?.getOrNull(currentCustomProxy)?.let { proxy ->
                                proxy.url?.let { proxyUrl ->
                                    (proxyUrl.toUri().takeIf { it.host != null } ?: "https://$proxyUrl".toUri()).let { uri ->
                                        if (proxy.addQueryParams) {
                                            val source = uri.getQueryParameter("allow_source") == null
                                            val audio = uri.getQueryParameter("allow_audio_only") == null
                                            val lowLatency = uri.getQueryParameter("fast_bread") == null
                                            if (source || audio || lowLatency) {
                                                uri.buildUpon().apply {
                                                    if (source) {
                                                        appendQueryParameter("allow_source", "true")
                                                    }
                                                    if (audio) {
                                                        appendQueryParameter("allow_audio_only", "true")
                                                    }
                                                    if (lowLatency) {
                                                        appendQueryParameter("fast_bread", "true")
                                                    }
                                                }.build()
                                            } else uri
                                        } else uri
                                    }.toString().replace("\$channel", channelLogin)
                                }
                            }
                            if (proxyUrl == null) {
                                result = getQualities(playlist)
                                break
                            }
                        }
                    }
                    result
                } else {
                    var streamProxy = if (useStreamProxy) {
                        streamProxyList?.getOrNull(currentStreamProxy)
                    } else null
                    if (streamProxy != null) {
                        val proxyHost = streamProxy.host
                        val proxyPort = streamProxy.port
                        val proxyUser = streamProxy.username
                        val proxyPassword = streamProxy.password
                        val playlistUrl = if (streamProxy.proxyPlaybackAccessToken) {
                            var result: String
                            while (true) {
                                val newPlaylistUrl = try {
                                    xtraModule.playerRepository.loadStreamPlaylistUrl(this@StreamDownloadService, networkLibrary, gqlHeaders, channelLogin, randomDeviceId, xDeviceId, playerType, supportedCodecs, true, proxyHost, proxyPort, proxyUser, proxyPassword, false)
                                } catch (e: Exception) {
                                    null
                                }
                                if (!newPlaylistUrl.isNullOrBlank()) {
                                    result = newPlaylistUrl
                                    break
                                } else {
                                    currentStreamProxy += 1
                                    streamProxy = streamProxyList?.getOrNull(currentStreamProxy)
                                    if (streamProxy == null || !streamProxy.proxyPlaybackAccessToken) {
                                        result = playlistUrl
                                        break
                                    }
                                }
                            }
                            result
                        } else {
                            playlistUrl
                        }
                        if (streamProxy != null) {
                            if (streamProxy.proxyMultivariantPlaylist) {
                                var result: List<VideoQuality>
                                while (true) {
                                    val newPlaylist = loadPlaylist(playlistUrl, networkLibrary, true, proxyHost, proxyPort, proxyUser, proxyPassword)
                                    if (!newPlaylist.isNullOrBlank()) {
                                        result = getQualities(newPlaylist).ifEmpty { getQualities(playlist) }
                                        break
                                    } else {
                                        currentStreamProxy += 1
                                        streamProxy = streamProxyList?.getOrNull(currentStreamProxy)
                                        if (streamProxy == null || !streamProxy.proxyMultivariantPlaylist) {
                                            result = getQualities(playlist)
                                            break
                                        }
                                    }
                                }
                                result
                            } else {
                                val newPlaylist = loadPlaylist(playlistUrl, networkLibrary, false, null, null, null, null)
                                if (!newPlaylist.isNullOrBlank()) {
                                    getQualities(newPlaylist).ifEmpty { getQualities(playlist) }
                                } else {
                                    getQualities(playlist)
                                }
                            }
                        } else {
                            getQualities(playlist)
                        }
                    } else {
                        getQualities(playlist)
                    }
                }
                if (qualities.isNotEmpty()) {
                    val selectedQuality = if (!quality.isNullOrBlank()) {
                        val audio = if (quality.startsWith("audio", true)) {
                            qualities.find { it.name == VideoQuality.AUDIO_ONLY_QUALITY }
                        } else null
                        if (audio != null) {
                            audio
                        } else {
                            val targetQuality = quality.split("p")
                            targetQuality.getOrNull(0)?.takeWhile { it.isDigit() }?.toIntOrNull()?.let { targetResolution ->
                                val targetFps = targetQuality.getOrNull(1)?.takeWhile { it.isDigit() }?.toIntOrNull() ?: 30
                                val last = qualities.lastOrNull { it.name != VideoQuality.AUDIO_ONLY_QUALITY }
                                qualities.find { quality ->
                                    quality.resolution != null
                                            && ((targetResolution == quality.resolution
                                            && targetFps >= (quality.frameRate?.let { fps -> floor(fps) } ?: 30f))
                                            || targetResolution > quality.resolution
                                            || quality == last)
                                }
                            } ?: qualities.first()
                        }
                    } else qualities.first()
                    xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                        status = OfflineVideo.STATUS_DOWNLOADING
                    })
                    downloadProgress.isLive = true
                    sendNotification(offlineVideo, downloadProgress)
                    val done = try {
                        download(offlineVideo, downloadProgress, downloadJob, channelLogin, selectedQuality.url!!, path, networkLibrary)
                        true
                    } catch (e: CancellationException) {
                        ensureActive()
                        false
                    } catch (e: Exception) {
                        Log.e("StreamDownloadService", "Download failed", e)
                        false
                    } finally {
                        MainScope().launch(Dispatchers.IO) {
                            downloadJob.chatReadWebSocket?.disconnect(null)
                        }
                    }
                    val waitForWifi = if (prefs().getBoolean(C.DOWNLOAD_WIFI_ONLY, false)) {
                        val connectivityManager = getSystemService(CONNECTIVITY_SERVICE) as ConnectivityManager
                        val networkCapabilities = connectivityManager.getNetworkCapabilities(connectivityManager.activeNetwork)
                        networkCapabilities != null && networkCapabilities.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR)
                    } else false
                    if (waitForWifi) {
                        throw Exception()
                    }
                    val continueDownloading = endWait == null || endWait > 0
                    if (done) {
                        if (offlineVideo.downloadChat && !offlineVideo.chatUrl.isNullOrBlank()) {
                            val chatUrl = offlineVideo.chatUrl!!
                            SafUtils.truncateFile(contentResolver, chatUrl, downloadProgress.chatBytes)
                            SafUtils.openOutputStream(contentResolver, chatUrl, append = true).bufferedWriter().use { fileWriter ->
                                if (downloadProgress.liveCommentsArrayStarted) {
                                    fileWriter.write("]")
                                }
                                fileWriter.write("}")
                            }
                        }
                        if (continueDownloading) {
                            offlineVideos.remove(offlineVideo)
                            activeDownloads.remove(downloadProgress)
                            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                                status = OfflineVideo.STATUS_DOWNLOADED
                            })
                            val oldId = offlineVideo.id
                            val newId = xtraModule.offlineVideosRepository.save(OfflineVideo(
                                channelId = offlineVideo.channelId,
                                channelLogin = offlineVideo.channelLogin,
                                channelName = offlineVideo.channelName,
                                channelLogo = offlineVideo.channelLogo,
                                downloadPath = offlineVideo.downloadPath,
                                status = OfflineVideo.STATUS_WAITING_FOR_STREAM,
                                quality = offlineVideo.quality,
                                downloadChat = offlineVideo.downloadChat,
                                downloadChatEmotes = offlineVideo.downloadChatEmotes,
                                live = true
                            )).toInt()
                            val newVideo = xtraModule.offlineVideosRepository.getById(newId)!!
                            val newDownloadProgress = DownloadProgress(
                                id = newId,
                                bytes = offlineVideo.bytes,
                                chatBytes = offlineVideo.chatBytes,
                                lastSegmentUrl = offlineVideo.lastSegmentUrl,
                                liveCommentsArrayStarted = offlineVideo.liveCommentsArrayStarted,
                            )
                            offlineVideo = newVideo
                            downloadProgress = newDownloadProgress
                            downloadJob.id = newId
                            offlineVideos.add(offlineVideo)
                            activeDownloads.add(downloadProgress)
                            sendNotification(offlineVideo, downloadProgress)
                            notificationManager?.cancel(oldId)
                        }
                    } else {
                        if (continueDownloading) {
                            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                                status = OfflineVideo.STATUS_WAITING_FOR_STREAM
                            })
                        }
                    }
                    endTime = endWait?.let { System.currentTimeMillis() + it }
                    if (continueDownloading) {
                        playlistUrl = xtraModule.playerRepository.loadStreamPlaylistUrl(this@StreamDownloadService, networkLibrary, gqlHeaders, channelLogin, randomDeviceId, xDeviceId, playerType, supportedCodecs, false, null, null, null, null, false)
                    }
                }
            }
            val currentTime = System.currentTimeMillis()
            if (endTime == null || currentTime < endTime) {
                val timeTaken = currentTime - startTime
                if (timeTaken < offlineCheck) {
                    delay((offlineCheck - timeTaken).milliseconds)
                }
                startTime = System.currentTimeMillis()
            } else {
                break
            }
        }
    }

    private suspend fun loadPlaylist(playlistUrl: String, networkLibrary: String?, useProxy: Boolean, proxyHost: String?, proxyPort: Int?, proxyUser: String?, proxyPassword: String?): String? = withContext(Dispatchers.IO) {
        val useProxy = useProxy && !proxyHost.isNullOrBlank() && proxyPort != null
        try {
            when {
                networkLibrary == C.HTTP_ENGINE && xtraModule.httpEngine.value != null -> @SuppressLint("NewApi") {
                    val httpEngine = if (useProxy) {
                        val proxyHeaders = if (!proxyUser.isNullOrBlank() && !proxyPassword.isNullOrBlank()) {
                            listOf(android.util.Pair("Proxy-Authorization", Base64.encodeToString("$proxyUser:$proxyPassword".toByteArray(), Base64.NO_WRAP)))
                        } else emptyList()
                        val builder = HttpEngine.Builder(application)
                        try {
                            builder.setProxyOptions(ProxyOptions.fromProxyList(
                                listOf(
                                    android.net.http.Proxy.createHttpProxy(
                                        android.net.http.Proxy.SCHEME_HTTP,
                                        proxyHost,
                                        proxyPort,
                                        xtraModule.cronetExecutor.value,
                                        object : android.net.http.Proxy.HttpConnectCallback {
                                            override fun onBeforeRequest(request: android.net.http.Proxy.HttpConnectCallback.Request) {
                                                request.proceed(proxyHeaders)
                                            }

                                            override fun onResponseReceived(responseHeaders: List<android.util.Pair<String?, String?>?>, statusCode: Int): Int {
                                                return android.net.http.Proxy.HttpConnectCallback.RESPONSE_ACTION_PROCEED
                                            }
                                        }
                                    )
                                ),
                                ProxyOptions.ALL_PROXIES_FAILED_BEHAVIOR_DISALLOW_DIRECT
                            ))
                        } catch (e: NoClassDefFoundError) {
                            null
                        }?.build()
                    } else {
                        xtraModule.httpEngine.value!!
                    }
                    if (httpEngine != null) {
                        val response = suspendCancellableCoroutine { continuation ->
                            val timeout = NetworkUtils.HttpEngineTimeout(CRONET_TIMEOUT)
                            val request = httpEngine.newUrlRequestBuilder(
                                playlistUrl,
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
                            response.body.decodeToString()
                        } else null
                    } else {
                        okHttpClient.value.newBuilder().apply {
                            proxy(Proxy(Proxy.Type.HTTP, InetSocketAddress(proxyHost, proxyPort!!)))
                            if (!proxyUser.isNullOrBlank() && !proxyPassword.isNullOrBlank()) {
                                proxyAuthenticator { _, response ->
                                    response.request.newBuilder().header("Proxy-Authorization", Credentials.basic(proxyUser, proxyPassword)).build()
                                }
                            }
                        }.build().newCall(Request.Builder().url(playlistUrl).build()).executeAsync().use { response ->
                            if (response.isSuccessful) {
                                response.body.string()
                            } else null
                        }
                    }
                }
                networkLibrary == C.CRONET && xtraModule.cronetEngine.value != null -> {
                    val cronetEngine = if (useProxy) {
                        if (CronetProvider.getAllProviders(application).any { it.isEnabled }) {
                            val proxyHeaders = if (!proxyUser.isNullOrBlank() && !proxyPassword.isNullOrBlank()) {
                                mapOf("Proxy-Authorization" to Base64.encodeToString("$proxyUser:$proxyPassword".toByteArray(), Base64.NO_WRAP)).entries.toList()
                            } else emptyList()
                            val builder = CronetEngine.Builder(application).apply {
                                val userAgent = "Cronet/" + defaultUserAgent.substringAfter("Cronet/", "").substringBefore(')')
                                setUserAgent(userAgent)
                                @QuicOptions.Experimental
                                setQuicOptions(QuicOptions.builder().setHandshakeUserAgent(userAgent).build())
                            }
                            try {
                                @org.chromium.net.ProxyOptions.Experimental
                                builder.setProxyOptions(org.chromium.net.ProxyOptions(
                                    listOf(
                                        org.chromium.net.Proxy(
                                            org.chromium.net.Proxy.HTTP,
                                            proxyHost,
                                            proxyPort,
                                            xtraModule.cronetExecutor.value,
                                            object : org.chromium.net.Proxy.Callback() {
                                                override fun onBeforeTunnelRequest(request: Request) {
                                                    request.proceed(proxyHeaders)
                                                }

                                                override fun onTunnelHeadersReceived(responseHeaders: List<Map.Entry<String?, String?>?>, statusCode: Int): Boolean {
                                                    return true
                                                }
                                            }
                                        )
                                    )
                                ))
                            } catch (e: UnsupportedOperationException) {
                                null
                            }?.build()
                        } else null
                    } else {
                        xtraModule.cronetEngine.value!!
                    }
                    if (cronetEngine != null) {
                        val response = suspendCancellableCoroutine { continuation ->
                            val timeout = NetworkUtils.CronetTimeout(CRONET_TIMEOUT)
                            val request = cronetEngine.newUrlRequestBuilder(
                                playlistUrl,
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
                            response.body.decodeToString()
                        } else null
                    } else {
                        okHttpClient.value.newBuilder().apply {
                            proxy(Proxy(Proxy.Type.HTTP, InetSocketAddress(proxyHost, proxyPort!!)))
                            if (!proxyUser.isNullOrBlank() && !proxyPassword.isNullOrBlank()) {
                                proxyAuthenticator { _, response ->
                                    response.request.newBuilder().header("Proxy-Authorization", Credentials.basic(proxyUser, proxyPassword)).build()
                                }
                            }
                        }.build().newCall(Request.Builder().url(playlistUrl).build()).executeAsync().use { response ->
                            if (response.isSuccessful) {
                                response.body.string()
                            } else null
                        }
                    }
                }
                else -> {
                    val okHttpClient = if (useProxy) {
                        okHttpClient.value.newBuilder().apply {
                            proxy(Proxy(Proxy.Type.HTTP, InetSocketAddress(proxyHost, proxyPort)))
                            if (!proxyUser.isNullOrBlank() && !proxyPassword.isNullOrBlank()) {
                                proxyAuthenticator { _, response ->
                                    response.request.newBuilder().header("Proxy-Authorization", Credentials.basic(proxyUser, proxyPassword)).build()
                                }
                            }
                        }.build()
                    } else {
                        okHttpClient.value
                    }
                    okHttpClient.newCall(Request.Builder().url(playlistUrl).build()).executeAsync().use { response ->
                        if (response.isSuccessful) {
                            response.body.string()
                        } else null
                    }
                }
            }
        } catch (e: Exception) {
            null
        }
    }

    private suspend fun getQualities(playlist: String): List<VideoQuality> = withContext(Dispatchers.IO) {
        val stableVariantIds = Regex("STABLE-VARIANT-ID=\"(.+?)\"").findAll(playlist).mapNotNull { it.groups[1]?.value }.toMutableList()
        val resolutions = Regex("RESOLUTION=(\\d+x\\d+)").findAll(playlist).mapNotNull { it.groups[1]?.value }.toMutableList()
        val frameRates = Regex("FRAME-RATE=([\\d.]+)\\b").findAll(playlist).mapNotNull { it.groups[1]?.value?.toFloatOrNull() }.toMutableList()
        val bitrates = Regex("BANDWIDTH=(\\d+)\\b").findAll(playlist).mapNotNull { it.groups[1]?.value?.toIntOrNull() }.toMutableList()
        val codecs = Regex("CODECS=\"(.+?)\"").findAll(playlist).mapNotNull { it.groups[1]?.value }.toMutableList()
        val urls = Regex("https://.*\\.m3u8").findAll(playlist).map(MatchResult::value).toMutableList()
        val list = stableVariantIds.mapIndexedNotNull { index, variantId ->
            urls.getOrNull(index)?.let { url ->
                VideoQuality(variantId, resolutions.getOrNull(index)?.substringAfter('x')?.toIntOrNull(), frameRates.getOrNull(index), bitrates.getOrNull(index), codecs.getOrNull(index), url)
            }
        }
        list
            .sortedWith(
                compareByDescending<VideoQuality> { it.bitrate }
                    .thenByDescending { it.frameRate }
                    .thenByDescending { it.resolution }
            )
            .toMutableList().apply {
                find { it.name.equals("source", true) }?.let { source ->
                    remove(source)
                    add(0, VideoQuality(VideoQuality.SOURCE_QUALITY, source.resolution, source.frameRate, source.bitrate, source.codecs, source.url))
                }
                find { it.name?.startsWith("audio", true) == true }?.let { audio ->
                    remove(audio)
                    add(VideoQuality(VideoQuality.AUDIO_ONLY_QUALITY, audio.resolution, audio.frameRate, audio.bitrate, audio.codecs, audio.url))
                }
            }
    }

    private suspend fun downloadByteArray(networkLibrary: String?, url: String): ByteArray {
        return when {
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
                if (response.info.httpStatusCode !in 200..299) {
                    throw IOException("HTTP ${response.info.httpStatusCode}")
                }
                response.body
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
                if (response.info.httpStatusCode !in 200..299) {
                    throw IOException("HTTP ${response.info.httpStatusCode}")
                }
                response.body
            }
            else -> {
                okHttpClient.value.newCall(Request.Builder().url(url).build()).executeAsync().use { response ->
                    if (!response.isSuccessful) {
                        throw IOException("HTTP ${response.code}")
                    }
                    response.body.bytes()
                }
            }
        }
    }

    private suspend fun download(offlineVideo: OfflineVideo, downloadProgress: DownloadProgress, downloadJob: DownloadJob, channelLogin: String, sourceUrl: String, path: String, networkLibrary: String?) = withContext(Dispatchers.IO) {
        val isShared = path.toUri().scheme == ContentResolver.SCHEME_CONTENT
        val liveCheck = max(prefs().getString(C.DOWNLOAD_STREAM_LIVE_CHECK, "2")?.toLongOrNull() ?: 2L, 2L) * 1000L
        val downloadDate = System.currentTimeMillis()
        var startTime = System.currentTimeMillis()
        var lastUrl = downloadProgress.lastSegmentUrl
        var initSegmentUri: String?
        val playlist = try {
            val playlistBytes = downloadByteArray(networkLibrary, sourceUrl)
            playlistBytes.inputStream().use {
                PlaylistUtils.parseMediaPlaylist(it)
            }
        } catch (e: Exception) {
            Log.e("StreamDownloadService", "Error loading initial playlist", e)
            return@withContext
        }
        val firstUrls = if (playlist.segments.isNotEmpty()) {
            val urls = playlist.segments.takeLastWhile { it.uri != lastUrl }
            urls.lastOrNull()?.let { lastUrl = it.uri }
            val streamStartTime = urls.firstOrNull()?.programDateTime
            if (offlineVideo.downloadChat && !streamStartTime.isNullOrBlank()) {
                launch(Dispatchers.IO) {
                    startChatJob(offlineVideo, downloadProgress, downloadJob, channelLogin, path, downloadDate, streamStartTime, networkLibrary)
                }
            }
            initSegmentUri = playlist.initSegmentUri
            urls.map { it.uri }
        } else {
            return@withContext
        }
        val videoFileUri = if (!offlineVideo.url.isNullOrBlank() && SafUtils.fileExists(contentResolver, offlineVideo.url!!)) {
            val fileUri = offlineVideo.url!!
            SafUtils.truncateFile(contentResolver, fileUri, downloadProgress.bytes)
            fileUri
        } else {
            val ext = firstUrls.first().substringAfterLast(".").substringBefore("?")
            val mimeType = if (ext.equals("mp4", true)) "video/mp4" else "video/mp2t"
            val fileName = "${offlineVideo.channelLogin ?: ""}${offlineVideo.quality ?: ""}${downloadDate}.$ext"
            val fileUri = SafUtils.getOrCreateDocument(contentResolver, path, fileName, mimeType)
            val initSegmentBytes = initSegmentUri?.let { url ->
                try {
                    val bytes = downloadByteArray(networkLibrary, url)
                    SafUtils.openOutputStream(contentResolver, fileUri, append = true).use {
                        it.write(bytes)
                    }
                    bytes.size.toLong()
                } catch (e: Exception) {
                    null
                }
            }
            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                url = fileUri
                initSegmentBytes?.let {
                    bytes += it
                    downloadProgress.bytes += it
                }
            })
            if (offlineVideo.name.isNullOrBlank()) {
                launch(Dispatchers.IO) {
                    updateStreamInfo(offlineVideo, channelLogin, networkLibrary)
                }
            }
            fileUri
        }
        val downloadLimit = prefs().getInt(C.DOWNLOAD_CONCURRENT_LIMIT, 10)
        downloadProgress.lastSaved = System.currentTimeMillis()

        suspend fun downloadSegmentBatch(urls: List<String>) {
            if (urls.isEmpty()) return
            coroutineScope {
                val readySegments = ConcurrentHashMap<Int, ByteArray>()
                val wakeChannel = Channel<Unit>(Channel.CONFLATED)
                val writerJob = launch {
                    for (i in urls.indices) {
                        while (true) {
                            val data = readySegments.remove(i)
                            if (data != null) {
                                SafUtils.openOutputStream(contentResolver, videoFileUri, append = true).use {
                                    it.write(data)
                                }
                                downloadProgress.bytes += data.size
                                downloadProgress.lastSegmentUrl = urls[i]
                                break
                            }
                            wakeChannel.receive()
                        }
                        val currentTime = System.currentTimeMillis()
                        if (currentTime - downloadProgress.lastSaved >= 5000L) {
                            downloadProgress.lastSaved = currentTime
                            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                                bytes = downloadProgress.bytes
                                chatBytes = downloadProgress.chatBytes
                                lastSegmentUrl = downloadProgress.lastSegmentUrl
                                liveCommentsArrayStarted = downloadProgress.liveCommentsArrayStarted
                            })
                        }
                    }
                }

                val semaphore = Semaphore(downloadLimit)
                urls.mapIndexed { index, url ->
                    launch {
                        semaphore.acquire()
                        try {
                            val data = downloadByteArray(networkLibrary, url)
                            readySegments[index] = data
                            wakeChannel.trySend(Unit)
                        } catch (e: Exception) {
                            Log.e("StreamDownloadService", "Error downloading stream segment: $url", e)
                            readySegments[index] = ByteArray(0)
                            wakeChannel.trySend(Unit)
                        } finally {
                            semaphore.release()
                        }
                    }
                }.joinAll()
                writerJob.join()
            }
        }

        downloadSegmentBatch(firstUrls)

        while (true) {
            val nextPlaylist = try {
                val bytes = downloadByteArray(networkLibrary, sourceUrl)
                bytes.inputStream().use {
                    PlaylistUtils.parseMediaPlaylist(it)
                }
            } catch (e: Exception) {
                Log.e("StreamDownloadService", "Error reloading live playlist", e)
                null
            }

            if (nextPlaylist != null && nextPlaylist.segments.isNotEmpty()) {
                val urls = nextPlaylist.segments.map { it.uri }.takeLastWhile { it != lastUrl }
                urls.lastOrNull()?.let { lastUrl = it }
                downloadSegmentBatch(urls)
                if (nextPlaylist.end) {
                    return@withContext
                }
            } else if (nextPlaylist == null) {
                // If playlist fetching failed temporarily, retry after delay
            } else {
                return@withContext
            }
            val timeTaken = System.currentTimeMillis() - startTime
            if (timeTaken < liveCheck) {
                delay((liveCheck - timeTaken).milliseconds)
            }
            startTime = System.currentTimeMillis()
        }
    }

    private suspend fun updateStreamInfo(offlineVideo: OfflineVideo, channelLogin: String, networkLibrary: String?) = withContext(Dispatchers.IO) {
        var attempt = 1
        while (attempt <= 10) {
            delay(10.seconds)
            val channelId = offlineVideo.channelId
            val stream = try {
                xtraModule.graphQLRepository.loadQueryUsersStream(
                    networkLibrary = networkLibrary,
                    headers = TwitchApiHelper.getGQLHeaders(this@StreamDownloadService),
                    ids = channelId?.let { listOf(it) },
                    logins = if (channelId.isNullOrBlank()) listOf(channelLogin) else null,
                ).data!!.users?.firstOrNull()?.let {
                    Stream(
                        id = it.stream?.id,
                        channelId = it.id,
                        channelLogin = it.login,
                        channelName = it.displayName,
                        channelImageURL = it.profileImageURL,
                        gameId = it.stream?.game?.id,
                        gameSlug = it.stream?.game?.slug,
                        gameName = it.stream?.game?.displayName,
                        title = it.stream?.broadcaster?.broadcastSettings?.title,
                        thumbnailURL = it.stream?.previewImageURL,
                        createdAt = it.stream?.createdAt?.toString(),
                        viewerCount = it.stream?.viewersCount,
                        tags = it.stream?.freeformTags?.mapNotNull { tag -> tag.name },
                    )
                }
            } catch (e: Exception) {
                val helixHeaders = TwitchApiHelper.getHelixHeaders(this@StreamDownloadService)
                if (helixHeaders[C.HEADER_TOKEN].isNullOrBlank()) throw Exception()
                try {
                    xtraModule.helixRepository.getStreams(
                        networkLibrary = networkLibrary,
                        headers = helixHeaders,
                        ids = channelId?.let { listOf(it) },
                        logins = if (channelId.isNullOrBlank()) listOf(channelLogin) else null
                    ).data.firstOrNull()?.let {
                        Stream(
                            id = it.id,
                            channelId = it.channelId,
                            channelLogin = it.channelLogin,
                            channelName = it.channelName,
                            gameId = it.gameId,
                            gameName = it.gameName,
                            title = it.title,
                            thumbnailURL = it.thumbnailURL,
                            createdAt = it.startedAt,
                            viewerCount = it.viewerCount,
                            tags = it.tags,
                        )
                    }
                } catch (e: Exception) {
                    null
                }
            }
            if (stream != null) {
                val downloadedThumbnail = stream.id.takeIf { !it.isNullOrBlank() }?.let { id ->
                    stream.thumbnail.takeIf { !it.isNullOrBlank() }?.let { url ->
                        val filesDir = filesDir.path
                        File(filesDir, "thumbnails").mkdir()
                        val filePath = filesDir + File.separator + "thumbnails" + File.separator + id
                        launch(Dispatchers.IO) {
                            try {
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
                                            FileOutputStream(filePath).use {
                                                it.write(response.body)
                                            }
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
                                            FileOutputStream(filePath).use {
                                                it.write(response.body)
                                            }
                                        }
                                    }
                                    else -> {
                                        okHttpClient.value.newCall(Request.Builder().url(url).build()).executeAsync().use { response ->
                                            if (response.isSuccessful) {
                                                FileOutputStream(filePath).use { outputStream ->
                                                    response.body.byteStream().use { inputStream ->
                                                        inputStream.copyTo(outputStream)
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            } catch (e: Exception) {

                            }
                        }
                        filePath
                    }
                }
                xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                    name = stream.title
                    thumbnail = downloadedThumbnail
                    gameId = stream.gameId
                    gameSlug = stream.gameSlug
                    gameName = stream.gameName
                    uploadDate = stream.createdAt?.let { Instant.parseOrNull(it)?.toEpochMilliseconds()?.takeIf { ms -> ms > 0 } }
                })
                break
            }
            attempt += 1
        }
    }

    private suspend fun startChatJob(offlineVideo: OfflineVideo, downloadProgress: DownloadProgress, downloadJob: DownloadJob, channelLogin: String, path: String, downloadDate: Long, streamStartTime: String, networkLibrary: String?) = withContext(Dispatchers.IO) {
        val resumed = !offlineVideo.chatUrl.isNullOrBlank() && downloadProgress.chatBytes > 0
        val fileUri = if (resumed && SafUtils.fileExists(contentResolver, offlineVideo.chatUrl!!)) {
            offlineVideo.chatUrl!!
        } else {
            val fileName = "${channelLogin}${offlineVideo.quality ?: ""}${downloadDate}_chat.json"
            val fileUri = SafUtils.getOrCreateDocument(contentResolver, path, fileName, "application/json")
            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                chatUrl = fileUri
            })
            fileUri
        }
        val downloadEmotes = offlineVideo.downloadChatEmotes
        val gqlHeaders = TwitchApiHelper.getGQLHeaders(this@StreamDownloadService, true)
        val helixHeaders = TwitchApiHelper.getHelixHeaders(this@StreamDownloadService)
        val emoteQuality = prefs().getString(C.CHAT_IMAGE_QUALITY, "4") ?: "4"
        val useWebp = prefs().getBoolean(C.CHAT_USE_WEBP, true)
        val channelId = offlineVideo.channelId
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
                        val emotes = xtraModule.playerRepository.loadFFZEmotes(response, useWebp)
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
        val savedTwitchEmotes = mutableListOf<String>()
        val savedBadges = mutableListOf<Pair<String, String>>()
        val savedEmotes = mutableListOf<String>()
        if (resumed) {
            SafUtils.truncateFile(contentResolver, fileUri, downloadProgress.chatBytes)
            SafUtils.openOutputStream(contentResolver, fileUri, append = true).bufferedWriter().use { writer ->
                if (downloadProgress.liveCommentsArrayStarted) {
                    writer.write("]")
                }
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
                Log.w("StreamDownloadService", "Error reading existing stream chat", e)
            }
            SafUtils.truncateFile(contentResolver, fileUri, downloadProgress.chatBytes)
        } else {
            SafUtils.openOutputStream(contentResolver, fileUri, append = false).bufferedWriter().use { writer ->
                writer.write("{".also { position += 1 })
                writer.write("\"video\":".also { position += it.length })
                writer.write(
                    buildJsonObject {
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
                writer.write("\"liveStartTime\":\"$streamStartTime\"".also { position += it.length })
            }
        }
        downloadProgress.chatBytes = position
        downloadJob.chatReadWebSocket = ChatReadWebSocket(
            channelLogin = channelLogin,
            trustManager = xtraModule.trustManager,
            listener = object : ChatReadWebSocket.Listener {
                override suspend fun onChatMessage(message: ChatUtils.IRCMessage, userNotice: Boolean) {
                    saveMessage(offlineVideo, downloadProgress, message, fileUri, downloadEmotes, networkLibrary, emoteQuality, savedTwitchEmotes, savedBadges, savedEmotes, globalBadgeList, channelBadgeList, cheerEmoteList, emoteList)
                }

                override suspend fun onClearMessage(message: ChatUtils.IRCMessage) {
                    saveMessage(offlineVideo, downloadProgress, message, fileUri, downloadEmotes, networkLibrary, emoteQuality, savedTwitchEmotes, savedBadges, savedEmotes, globalBadgeList, channelBadgeList, cheerEmoteList, emoteList)
                }

                override suspend fun onClearChat(message: ChatUtils.IRCMessage) {
                    saveMessage(offlineVideo, downloadProgress, message, fileUri, downloadEmotes, networkLibrary, emoteQuality, savedTwitchEmotes, savedBadges, savedEmotes, globalBadgeList, channelBadgeList, cheerEmoteList, emoteList)
                }

                override suspend fun onNotice(message: ChatUtils.IRCMessage) {
                    saveMessage(offlineVideo, downloadProgress, message, fileUri, downloadEmotes, networkLibrary, emoteQuality, savedTwitchEmotes, savedBadges, savedEmotes, globalBadgeList, channelBadgeList, cheerEmoteList, emoteList)
                }
            }
        ).apply { connect(this@withContext) }
    }

    private suspend fun saveMessage(offlineVideo: OfflineVideo, downloadProgress: DownloadProgress, message: ChatUtils.IRCMessage, fileUri: String, downloadEmotes: Boolean, networkLibrary: String?, emoteQuality: String, savedTwitchEmotes: MutableList<String>, savedBadges: MutableList<Pair<String, String>>, savedEmotes: MutableList<String>, globalBadgeList: List<TwitchBadge>, channelBadgeList: List<TwitchBadge>, cheerEmoteList: List<CheerEmote>, emoteList: List<Emote>) = withContext(Dispatchers.IO) {
        var position = downloadProgress.chatBytes
        var liveCommentsArrayStarted = downloadProgress.liveCommentsArrayStarted
        SafUtils.openOutputStream(contentResolver, fileUri, append = true).bufferedWriter().use { writer ->
            writer.write(",".also { position += 1 })
            if (!liveCommentsArrayStarted) {
                liveCommentsArrayStarted = true
                writer.write("\"liveComments\":".also { position += it.length })
                writer.write("[".also { position += 1 })
            }
            writer.write(JsonPrimitive(message.fullMessage).toString().also { position += it.toByteArray().size })
        }
        if (downloadEmotes) {
            val chatMessage = when (message.command) {
                "PRIVMSG", "USERNOTICE" -> ChatUtils.parseChatMessage(message)
                "CLEARMSG" -> ChatUtils.parseClearMessage(message)
                "CLEARCHAT" -> ChatUtils.parseClearChat(this@StreamDownloadService, message)
                "NOTICE" -> ChatUtils.parseNotice(message)
                else -> null
            }
            if (chatMessage != null) {
                val twitchEmotes = mutableListOf<TwitchEmote>()
                val twitchBadges = mutableListOf<TwitchBadge>()
                val cheerEmotes = mutableListOf<CheerEmote>()
                val emotes = mutableListOf<Emote>()
                chatMessage.emotes?.forEach {
                    if (it.id != null && !savedTwitchEmotes.contains(it.id)) {
                        savedTwitchEmotes.add(it.id)
                        twitchEmotes.add(it)
                    }
                }
                chatMessage.badges?.forEach {
                    val pair = Pair(it.setId, it.version)
                    if (!savedBadges.contains(pair)) {
                        savedBadges.add(pair)
                        val badge = channelBadgeList.find { badge -> badge.setId == it.setId && badge.version == it.version }
                            ?: globalBadgeList.find { badge -> badge.setId == it.setId && badge.version == it.version }
                        if (badge != null) {
                            twitchBadges.add(badge)
                        }
                    }
                }
                chatMessage.message?.split(" ")?.forEach { word ->
                    if (!savedEmotes.contains(word)) {
                        val cheerEmote = if (chatMessage.bits != null) {
                            val bitsCount = word.takeLastWhile { it.isDigit() }
                            val bitsName = word.substringBeforeLast(bitsCount)
                            if (bitsCount.isNotEmpty()) {
                                cheerEmoteList.findLast { it.name.equals(bitsName, true) && it.minBits <= bitsCount.toInt() }
                            } else null
                        } else null
                        if (cheerEmote != null) {
                            savedEmotes.add(word)
                            cheerEmotes.add(cheerEmote)
                        } else {
                            val emote = emoteList.find { it.name == word }
                            if (emote != null) {
                                savedEmotes.add(word)
                                emotes.add(emote)
                            }
                        }
                    }
                }
                if (twitchEmotes.isNotEmpty() || twitchBadges.isNotEmpty() || cheerEmotes.isNotEmpty() || emotes.isNotEmpty()) {
                    SafUtils.openOutputStream(contentResolver, fileUri, append = true).bufferedWriter().use { writer ->
                        writer.write("]".also { position += 1 })
                    }
                    liveCommentsArrayStarted = false
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
        downloadProgress.chatBytes = position
        downloadProgress.liveCommentsArrayStarted = liveCommentsArrayStarted
        val currentTime = System.currentTimeMillis()
        if (currentTime - downloadProgress.lastSaved >= 5000L) {
            downloadProgress.lastSaved = currentTime
            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                bytes = downloadProgress.bytes
                chatBytes = downloadProgress.chatBytes
                lastSegmentUrl = downloadProgress.lastSegmentUrl
                this.liveCommentsArrayStarted = downloadProgress.liveCommentsArrayStarted
            })
        }
    }

    private fun sendNotification(offlineVideo: OfflineVideo, downloadProgress: DownloadProgress) {
        val notification = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            Notification.Builder(this, getString(R.string.notification_downloads_channel_id))
        } else {
            @Suppress("DEPRECATION")
            Notification.Builder(this)
        }.apply {
            setContentTitle(ContextCompat.getString(this@StreamDownloadService, if (downloadProgress.isLive) {
                R.string.downloading
            } else {
                R.string.download_waiting_for_stream
            }))
            setContentText(offlineVideo.channelName)
            setSmallIcon(android.R.drawable.stat_sys_download)
            setGroup(GROUP_KEY)
            setOngoing(true)
            setOnlyAlertOnce(true)
            setContentIntent(
                PendingIntent.getActivity(
                    this@StreamDownloadService,
                    offlineVideo.id,
                    Intent(this@StreamDownloadService, MainActivity::class.java).apply {
                        flags = Intent.FLAG_ACTIVITY_CLEAR_TOP
                        action = MainActivity.INTENT_OPEN_DOWNLOADS_TAB
                    },
                    PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
                )
            )
            addAction(
                Notification.Action.Builder(
                    Icon.createWithResource(this@StreamDownloadService, android.R.drawable.ic_delete),
                    ContextCompat.getString(this@StreamDownloadService, R.string.stop),
                    PendingIntent.getService(
                        this@StreamDownloadService,
                        REQUEST_CODE_STOP,
                        Intent(this@StreamDownloadService, StreamDownloadService::class.java).apply {
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
            INTENT_STOP, INTENT_CANCEL -> {
                val videoId = intent.getIntExtra(KEY_VIDEO_ID, 0)
                stoppedVideoIds.add(videoId)
                downloadJobs.find { it.id == videoId }?.job?.cancel()
                val offlineVideo = offlineVideos.find { it.id == videoId }
                val downloadProgress = activeDownloads.find { it.id == videoId }
                if (offlineVideo != null && downloadProgress != null) {
                    offlineVideos.remove(offlineVideo)
                    activeDownloads.remove(downloadProgress)
                    if (intent.action == INTENT_STOP) {
                        lifecycleScope.launch(Dispatchers.IO) {
                            xtraModule.offlineVideosRepository.update(offlineVideo.apply {
                                status = OfflineVideo.STATUS_PENDING
                                bytes = downloadProgress.bytes
                                chatBytes = downloadProgress.chatBytes
                                lastSegmentUrl = downloadProgress.lastSegmentUrl
                                liveCommentsArrayStarted = downloadProgress.liveCommentsArrayStarted
                            })
                        }
                    }
                } else if (intent.action == INTENT_STOP) {
                    lifecycleScope.launch(Dispatchers.IO) {
                        val dbVideo = xtraModule.offlineVideosRepository.getById(videoId)
                        if (dbVideo != null && (dbVideo.status == OfflineVideo.STATUS_DOWNLOADING || dbVideo.status == OfflineVideo.STATUS_QUEUED || dbVideo.status == OfflineVideo.STATUS_WAITING_FOR_STREAM)) {
                            dbVideo.status = OfflineVideo.STATUS_PENDING
                            xtraModule.offlineVideosRepository.update(dbVideo)
                        }
                    }
                }
                if (intent.action == INTENT_STOP) {
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
            INTENT_START -> {
                val videoId = intent.getIntExtra(KEY_VIDEO_ID, 0)
                stoppedVideoIds.remove(videoId)
                start(videoId)
            }
        }
        return START_STICKY
    }

    override fun onBind(intent: Intent): IBinder {
        super.onBind(intent)
        return ServiceBinder()
    }

    inner class ServiceBinder : Binder() {
        fun getService() = this@StreamDownloadService
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

        private const val REQUEST_CODE_STOP = 0

        const val KEY_VIDEO_ID = "videoId"

        const val INTENT_STOP = "com.github.andreyasadchy.xtra.STOP"
        const val INTENT_CANCEL = "com.github.andreyasadchy.xtra.CANCEL"
        const val INTENT_START = "com.github.andreyasadchy.xtra.START_VIDEO_DOWNLOAD_SERVICE"
    }
}