package com.github.andreyasadchy.xtra.ui.chat

import com.github.andreyasadchy.xtra.model.chat.ChatMessage
import com.github.andreyasadchy.xtra.model.chat.VideoChatMessage
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlin.math.max
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

class ChatReplayManagerLocal(
    private val createdAt: Long?,
    private val getCurrentPosition: () -> Long?,
    private val getCurrentSpeed: () -> Float?,
    private val coroutineScope: CoroutineScope,
    private val listener: ChatReplayManager.Listener,
) {
    private var liveMessages: List<ChatMessage>? = null
    private var messages: List<VideoChatMessage>? = null
    private var startTime = 0L
    private val liveList = mutableListOf<ChatMessage>()
    private val list = mutableListOf<VideoChatMessage>()
    private var started = false
    private var isLoading = false
    private var loadJob: Job? = null
    private var messageJob: Job? = null
    private var lastCheckedPosition = 0L
    private var playbackSpeed: Float? = null
    var isActive = true

    fun setMessages(newLiveMessages: List<ChatMessage>, newMessages: List<VideoChatMessage>, newStartTime: Long) {
        if (newLiveMessages.isNotEmpty()) {
            liveMessages = newLiveMessages
            if (createdAt != null) {
                startTime = newStartTime - createdAt
            }
        } else {
            messages = newMessages
            startTime = newStartTime
        }
        if (started) {
            start()
        }
    }

    fun startLoad() {
        if (!started) {
            started = true
            if (!liveMessages.isNullOrEmpty() || !messages.isNullOrEmpty()) {
                start()
            }
        }
    }

    fun start() {
        val currentPosition = getCurrentPosition() ?: 0
        lastCheckedPosition = currentPosition
        playbackSpeed = getCurrentSpeed()
        synchronized(liveList) {
            liveList.clear()
        }
        synchronized(list) {
            list.clear()
        }
        coroutineScope.launch {
            listener.clearMessages()
        }
        load(currentPosition + startTime)
    }

    fun stop() {
        loadJob?.cancel()
        messageJob?.cancel()
        isActive = false
    }

    private fun load(position: Long) {
        isLoading = true
        loadJob = coroutineScope.launch(Dispatchers.IO) {
            try {
                messageJob?.cancel()
                if (!liveMessages.isNullOrEmpty()) {
                    liveMessages?.let { messages ->
                        val filtered = messages.filter { message ->
                            val messageOffset = if (createdAt != null && message.timestamp != null) {
                                message.timestamp - createdAt
                            } else {
                                null
                            }
                            messageOffset != null && messageOffset >= (max(position - 20000, 0))
                        }
                        synchronized(liveList) {
                            liveList.addAll(filtered)
                        }
                    }
                } else {
                    messages?.let { messages ->
                        val filtered = messages.filter { message ->
                            val messageOffset = if (createdAt != null && !message.createdAt.isNullOrBlank()) {
                                Instant.parseOrNull(message.createdAt)?.toEpochMilliseconds()?.takeIf { ms -> ms > 0 }?.minus(createdAt)
                            } else {
                                null
                            } ?: message.offsetSeconds?.times(1000L)
                            messageOffset != null && messageOffset >= (max(position - 20000, 0))
                        }
                        synchronized(list) {
                            list.addAll(filtered)
                        }
                    }
                }
                isLoading = false
                startJob()
            } catch (e: Exception) {
                isLoading = false
            }
        }
    }

    private fun startJob() {
        if (messageJob?.isActive == true) {
            return
        }
        messageJob = coroutineScope.launch {
            var burstCount = 0
            while (this@ChatReplayManagerLocal.isActive && isActive) {
                if (!liveMessages.isNullOrEmpty()) {
                    val message = synchronized(liveList) { liveList.firstOrNull() } ?: break
                    val messageOffset = if (createdAt != null && message.timestamp != null) {
                        message.timestamp - createdAt
                    } else {
                        null
                    }
                    if (messageOffset != null) {
                        var currentPosition: Long = (getCurrentPosition() ?: 0) + startTime
                        lastCheckedPosition = currentPosition - startTime

                        if (messageOffset < currentPosition - 30_000L) {
                            synchronized(liveList) { liveList.remove(message) }
                            continue
                        }

                        while (currentPosition < messageOffset) {
                            burstCount = 0
                            val speed = playbackSpeed?.takeIf { it > 0f } ?: 1f
                            val timeLeft = (messageOffset - currentPosition).div(speed).toLong()
                            val waitTime = timeLeft.coerceIn(30L, 1000L)
                            delay(waitTime.milliseconds)
                            if (!this@ChatReplayManagerLocal.isActive || !isActive) {
                                break
                            }
                            val pos = getCurrentPosition() ?: 0
                            lastCheckedPosition = pos
                            currentPosition = pos + startTime
                        }
                        if (!this@ChatReplayManagerLocal.isActive || !isActive) {
                            break
                        }
                        listener.onChatMessage(message)
                        burstCount++
                        if (burstCount >= 3) {
                            burstCount = 0
                            delay(16.milliseconds)
                        }
                    } else {
                        if (!this@ChatReplayManagerLocal.isActive || !isActive) {
                            break
                        }
                    }
                    synchronized(liveList) { liveList.remove(message) }
                } else {
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
                            if (!this@ChatReplayManagerLocal.isActive || !isActive) {
                                break
                            }
                            val pos = getCurrentPosition() ?: 0
                            lastCheckedPosition = pos
                            currentPosition = pos + startTime
                        }
                        if (!this@ChatReplayManagerLocal.isActive || !isActive) {
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
                    } else {
                        if (!this@ChatReplayManagerLocal.isActive || !isActive) {
                            break
                        }
                    }
                    synchronized(list) { list.remove(message) }
                }
            }
        }
    }

    fun updatePosition(position: Long) {
        if (started && (!liveMessages.isNullOrEmpty() || !messages.isNullOrEmpty()) && lastCheckedPosition != position) {
            if (position - lastCheckedPosition !in 0..20000) {
                loadJob?.cancel()
                messageJob?.cancel()
                synchronized(liveList) {
                    liveList.clear()
                }
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
        if (started && (!liveMessages.isNullOrEmpty() || !messages.isNullOrEmpty()) && playbackSpeed != speed) {
            playbackSpeed = speed
            messageJob?.cancel()
            startJob()
        }
    }
}