package com.github.andreyasadchy.xtra.ui.download

import android.content.Context
import android.content.Intent
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import com.github.andreyasadchy.xtra.XtraApp
import com.github.andreyasadchy.xtra.model.ui.OfflineVideo
import com.github.andreyasadchy.xtra.util.C
import com.github.andreyasadchy.xtra.util.prefs
import java.util.concurrent.TimeUnit

class DownloadRetryWorker(
    private val context: Context,
    parameters: WorkerParameters
) : CoroutineWorker(context, parameters) {

    override suspend fun doWork(): Result {
        val prefs = context.prefs()
        if (!prefs.getBoolean(C.DOWNLOAD_AUTO_RETRY, true)) {
            return Result.success()
        }

        val connectivityManager = context.getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
        val activeNetwork = connectivityManager.activeNetwork ?: return Result.retry()
        val networkCapabilities = connectivityManager.getNetworkCapabilities(activeNetwork) ?: return Result.retry()

        val wifiOnly = prefs.getBoolean(C.DOWNLOAD_WIFI_ONLY, false)
        val cellular = networkCapabilities.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR)
        if (wifiOnly && cellular) {
            return Result.retry()
        }

        val repository = (context.applicationContext as XtraApp).xtraModule.offlineVideosRepository
        val waitingDownloads = repository.getWaitingDownloads()
        for (video in waitingDownloads) {
            val intent = if (video.live) {
                Intent(context, StreamDownloadService::class.java).apply {
                    action = StreamDownloadService.INTENT_START
                    putExtra(StreamDownloadService.KEY_VIDEO_ID, video.id)
                }
            } else {
                Intent(context, VideoDownloadService::class.java).apply {
                    action = VideoDownloadService.INTENT_START
                    putExtra(VideoDownloadService.KEY_VIDEO_ID, video.id)
                }
            }
            try {
                context.startService(intent)
            } catch (e: Exception) {
                // Ignore service start failures if background restricted
            }
        }

        return Result.success()
    }

    companion object {
        private const val UNIQUE_WORK_NAME_ONE_TIME = "DownloadRetryWorkerOneTime"
        private const val UNIQUE_WORK_NAME_PERIODIC = "DownloadRetryWorkerPeriodic"

        fun enqueueRetry(context: Context, delaySeconds: Long = 10L) {
            val constraints = Constraints.Builder()
                .setRequiredNetworkType(NetworkType.CONNECTED)
                .build()

            val request = OneTimeWorkRequestBuilder<DownloadRetryWorker>()
                .setConstraints(constraints)
                .setInitialDelay(delaySeconds, TimeUnit.SECONDS)
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 15, TimeUnit.SECONDS)
                .build()

            WorkManager.getInstance(context).enqueueUniqueWork(
                UNIQUE_WORK_NAME_ONE_TIME,
                ExistingWorkPolicy.REPLACE,
                request
            )
        }

        fun enqueuePeriodic(context: Context) {
            val constraints = Constraints.Builder()
                .setRequiredNetworkType(NetworkType.CONNECTED)
                .build()

            val request = PeriodicWorkRequestBuilder<DownloadRetryWorker>(15, TimeUnit.MINUTES)
                .setConstraints(constraints)
                .build()

            WorkManager.getInstance(context).enqueueUniquePeriodicWork(
                UNIQUE_WORK_NAME_PERIODIC,
                ExistingPeriodicWorkPolicy.KEEP,
                request
            )
        }
    }
}
