package com.github.andreyasadchy.xtra.ui.chat

import com.github.andreyasadchy.xtra.model.chat.Badge
import com.github.andreyasadchy.xtra.model.chat.ChatMessage
import com.github.andreyasadchy.xtra.model.chat.TwitchEmote
import com.github.andreyasadchy.xtra.model.chat.VideoChatMessage
import com.github.andreyasadchy.xtra.repository.GraphQLRepository
import com.github.andreyasadchy.xtra.util.C
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import kotlin.math.max
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

class ChatReplayManager(
    private val networkLibrary: String?,
    private val gqlHeaders: Map<String, String>,
    private val graphQLRepository: GraphQLRepository,
    private val json: Json,
    private val enableIntegrity: Boolean,
    private val videoId: String,
    private val createdAt: Long?,
    private val startTime: Long,
    private val getCurrentPosition: () -> Long?,
    private val getCurrentSpeed: () -> Float?,
    private val coroutineScope: CoroutineScope,
    private val listener: Listener,
) {
    private var cursor: String? = null
    private val list = mutableListOf<VideoChatMessage>()
    private var started = false
    private var isLoading = false
    private var loadJob: Job? = null
    private var messageJob: Job? = null
    private var lastCheckedPosition = 0L
    private var playbackSpeed: Float? = null
    var isActive = true

    fun start() {
        if (!started) {
            started = true
            val currentPosition = getCurrentPosition() ?: 0
            lastCheckedPosition = currentPosition
            playbackSpeed = getCurrentSpeed()
            synchronized(list) {
                list.clear()
            }
            coroutineScope.launch {
                listener.clearMessages()
            }
            load(currentPosition + startTime)
        }
    }

    fun stop() {
        loadJob?.cancel()
        messageJob?.cancel()
        isActive = false
    }

    private fun load(position: Long? = null) {
        isLoading = true
        loadJob = coroutineScope.launch(Dispatchers.IO) {
            try {
                val response = if (position != null) {
                    graphQLRepository.loadQueryVideoComments(networkLibrary, gqlHeaders, videoId, offset = position.div(1000).toInt())
                } else {
                    graphQLRepository.loadQueryVideoComments(networkLibrary, gqlHeaders, videoId, cursor = cursor)
                }
                if (enableIntegrity) {
                    response.errors?.find { it.message == C.FAILED_INTEGRITY_CHECK }?.let {
                        listener.getIntegrityToken()
                        isLoading = false
                        return@launch
                    }
                }
                val comments = response.data!!.video!!.comments!!
                val messages = comments.edges!!.mapNotNull { comment ->
                    comment?.node.let { item ->
                        item?.message?.let { message ->
                            val chatMessage = StringBuilder()
                            val emotes = message.fragments?.mapNotNull { fragment ->
                                fragment?.text?.let { text ->
                                    fragment.emote?.emoteID?.let { id ->
                                        TwitchEmote(
                                            id = id,
                                            begin = chatMessage.codePointCount(0, chatMessage.length),
                                            end = chatMessage.codePointCount(0, chatMessage.length) + text.lastIndex
                                        )
                                    }.also { chatMessage.append(text) }
                                }
                            }
                            val badges = message.userBadges?.mapNotNull { badge ->
                                badge?.setID?.let { setId ->
                                    badge.version?.let { version ->
                                        Badge(
                                            setId = setId,
                                            version = version,
                                        )
                                    }
                                }
                            }
                            VideoChatMessage(
                                id = item.id,
                                offsetSeconds = item.contentOffsetSeconds,
                                createdAt = item.createdAt?.toString(),
                                userId = item.commenter?.id,
                                userLogin = item.commenter?.login,
                                userName = item.commenter?.displayName,
                                message = chatMessage.toString(),
                                color = message.userColor,
                                emotes = emotes,
                                badges = badges,
                                fullMsg = null
                            )
                        }
                    }
                }
                messageJob?.cancel()
                synchronized(list) {
                    list.addAll(messages)
                }
                cursor = if (comments.pageInfo?.hasNextPage != false) comments.edges.lastOrNull()?.cursor?.toString() else null
                isLoading = false
                startJob()
            } catch (e: Exception) {
                try {
                    val response = if (position != null) {
                        graphQLRepository.loadVideoMessages(networkLibrary, gqlHeaders, videoId, offset = position.div(1000).toInt())
                    } else {
                        graphQLRepository.loadVideoMessages(networkLibrary, gqlHeaders, videoId, cursor = cursor)
                    }
                    if (enableIntegrity) {
                        response.errors?.find { it.message == C.FAILED_INTEGRITY_CHECK }?.let {
                            listener.getIntegrityToken()
                            isLoading = false
                            return@launch
                        }
                    }
                    val comments = response.data!!.video.comments
                    val messages = comments.edges.mapNotNull { comment ->
                        comment.node.let { item ->
                            item.message?.let { message ->
                                val chatMessage = StringBuilder()
                                val emotes = message.fragments?.mapNotNull { fragment ->
                                    fragment.text?.let { text ->
                                        fragment.emote?.emoteID?.let { id ->
                                            TwitchEmote(
                                                id = id,
                                                begin = chatMessage.codePointCount(0, chatMessage.length),
                                                end = chatMessage.codePointCount(0, chatMessage.length) + text.lastIndex
                                            )
                                        }.also { chatMessage.append(text) }
                                    }
                                }
                                val badges = message.userBadges?.mapNotNull { badge ->
                                    badge.setID?.let { setId ->
                                        badge.version?.let { version ->
                                            Badge(
                                                setId = setId,
                                                version = version,
                                            )
                                        }
                                    }
                                }
                                VideoChatMessage(
                                    id = item.id,
                                    offsetSeconds = item.contentOffsetSeconds,
                                    createdAt = item.createdAt,
                                    userId = item.commenter?.id,
                                    userLogin = item.commenter?.login,
                                    userName = item.commenter?.displayName,
                                    message = chatMessage.toString(),
                                    color = message.userColor,
                                    emotes = emotes,
                                    badges = badges,
                                    fullMsg = json.encodeToString(item)
                                )
                            }
                        }
                    }
                    messageJob?.cancel()
                    synchronized(list) {
                        list.addAll(messages)
                    }
                    cursor = if (comments.pageInfo?.hasNextPage != false) comments.edges.lastOrNull()?.cursor else null
                    isLoading = false
                    startJob()
                } catch (e: Exception) {
                    isLoading = false
                }
            }
        }
    }

    private fun startJob() {
        if (messageJob?.isActive == true) {
            return
        }
        messageJob = coroutineScope.launch {
            var burstCount = 0
            while (this@ChatReplayManager.isActive && isActive) {
                val message = synchronized(list) { list.firstOrNull() } ?: break
                val messageOffset = if (createdAt != null && !message.createdAt.isNullOrBlank()) {
                    Instant.parseOrNull(message.createdAt)?.toEpochMilliseconds()?.takeIf { ms -> ms > 0 }?.minus(createdAt)
                } else {
                    null
                } ?: message.offsetSeconds?.times(1000L)
                if (messageOffset != null) {
                    var currentPosition: Long = (getCurrentPosition() ?: 0) + startTime
                    lastCheckedPosition = currentPosition - startTime

                    if (messageOffset < currentPosition - 30_000L) {
                        synchronized(list) { list.remove(message) }
                        continue
                    }

                    while (currentPosition < messageOffset) {
                        burstCount = 0
                        val speed = playbackSpeed?.takeIf { it > 0f } ?: 1f
                        val timeLeft = (messageOffset - currentPosition).div(speed).toLong()
                        val waitTime = timeLeft.coerceIn(30L, 1000L)
                        delay(waitTime.milliseconds)
                        if (!this@ChatReplayManager.isActive || !isActive) {
                            break
                        }
                        val pos = getCurrentPosition() ?: 0
                        lastCheckedPosition = pos
                        currentPosition = pos + startTime
                    }
                    if (!this@ChatReplayManager.isActive || !isActive) {
                        break
                    }
                    listener.onChatMessage(
                        ChatMessage(
                            type = ChatMessage.USER_MESSAGE,
                            id = message.id,
                            userId = message.userId,
                            userLogin = message.userLogin,
                            userName = message.userName,
                            message = message.message,
                            color = message.color,
                            emotes = message.emotes,
                            badges = message.badges,
                            bits = 0,
                            fullMsg = message.fullMsg
                        )
                    )
                    burstCount++
                    if (burstCount >= 3) {
                        burstCount = 0
                        delay(16.milliseconds)
                    }
                    val shouldLoad = synchronized(list) { list.size <= 25 }
                    if (shouldLoad && !cursor.isNullOrBlank() && !isLoading) {
                        load()
                    }
                } else {
                    if (!this@ChatReplayManager.isActive || !isActive) {
                        break
                    }
                }
                synchronized(list) { list.remove(message) }
            }
        }
    }

    fun updatePosition(position: Long) {
        if (started && lastCheckedPosition != position) {
            if (position - lastCheckedPosition !in 0..20000) {
                loadJob?.cancel()
                messageJob?.cancel()
                synchronized(list) {
                    list.clear()
                }
                coroutineScope.launch {
                    listener.clearMessages()
                }
                load(position + startTime)
            } else {
                messageJob?.cancel()
                startJob()
            }
            lastCheckedPosition = position
        }
    }

    fun updateSpeed(speed: Float) {
        if (started && playbackSpeed != speed) {
            playbackSpeed = speed
            messageJob?.cancel()
            startJob()
        }
    }

    interface Listener {
        suspend fun onChatMessage(message: ChatMessage) {}
        suspend fun clearMessages() {}
        suspend fun getIntegrityToken() {}
    }
}