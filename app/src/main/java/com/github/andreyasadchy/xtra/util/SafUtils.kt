package com.github.andreyasadchy.xtra.util

import android.content.ContentResolver
import android.net.Uri
import android.provider.DocumentsContract
import android.util.Log
import androidx.core.net.toUri
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.io.RandomAccessFile

object SafUtils {
    private const val TAG = "SafUtils"

    fun isContentUri(path: String): Boolean = path.toUri().scheme == ContentResolver.SCHEME_CONTENT

    fun getOrCreateDocument(contentResolver: ContentResolver, pathString: String, fileName: String, mimeType: String = "video/mp2t"): String {
        if (!isContentUri(pathString)) {
            val file = File(pathString, fileName)
            file.parentFile?.mkdirs()
            return file.absolutePath
        }

        val treeUri = pathString.toUri()
        val documentId = DocumentsContract.getTreeDocumentId(treeUri)
        val directoryUri = DocumentsContract.buildDocumentUriUsingTree(treeUri, documentId)

        // Query if document already exists
        val childrenUri = DocumentsContract.buildChildDocumentsUriUsingTree(treeUri, documentId)
        val projection = arrayOf(
            DocumentsContract.Document.COLUMN_DOCUMENT_ID,
            DocumentsContract.Document.COLUMN_DISPLAY_NAME
        )
        try {
            contentResolver.query(childrenUri, projection, null, null, null)?.use { cursor ->
                val idCol = cursor.getColumnIndexOrThrow(DocumentsContract.Document.COLUMN_DOCUMENT_ID)
                val nameCol = cursor.getColumnIndexOrThrow(DocumentsContract.Document.COLUMN_DISPLAY_NAME)
                while (cursor.moveToNext()) {
                    if (cursor.getString(nameCol) == fileName) {
                        val existingDocId = cursor.getString(idCol)
                        return DocumentsContract.buildDocumentUriUsingTree(treeUri, existingDocId).toString()
                    }
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "Error querying existing SAF child documents", e)
        }

        val created = try {
            DocumentsContract.createDocument(contentResolver, directoryUri, mimeType, fileName)
        } catch (e: Exception) {
            Log.w(TAG, "Error creating SAF document via DocumentsContract", e)
            null
        }

        if (created != null) {
            return created.toString()
        }

        val fallback = directoryUri.toString() + (if (!directoryUri.toString().endsWith("%3A")) "%2F" else "") + fileName
        return fallback
    }

    fun getOrCreateDirectory(contentResolver: ContentResolver, pathString: String, dirName: String): Pair<String, String> {
        if (!isContentUri(pathString)) {
            val dir = File(pathString, dirName)
            dir.mkdirs()
            return Pair(dir.absolutePath, dir.absolutePath)
        }

        val treeUri = pathString.toUri()
        val documentId = DocumentsContract.getTreeDocumentId(treeUri)
        val directoryUri = DocumentsContract.buildDocumentUriUsingTree(treeUri, documentId)

        val childrenUri = DocumentsContract.buildChildDocumentsUriUsingTree(treeUri, documentId)
        val projection = arrayOf(
            DocumentsContract.Document.COLUMN_DOCUMENT_ID,
            DocumentsContract.Document.COLUMN_DISPLAY_NAME,
            DocumentsContract.Document.COLUMN_MIME_TYPE
        )
        try {
            contentResolver.query(childrenUri, projection, null, null, null)?.use { cursor ->
                val idCol = cursor.getColumnIndexOrThrow(DocumentsContract.Document.COLUMN_DOCUMENT_ID)
                val nameCol = cursor.getColumnIndexOrThrow(DocumentsContract.Document.COLUMN_DISPLAY_NAME)
                val mimeCol = cursor.getColumnIndexOrThrow(DocumentsContract.Document.COLUMN_MIME_TYPE)
                while (cursor.moveToNext()) {
                    if (cursor.getString(nameCol) == dirName && cursor.getString(mimeCol) == DocumentsContract.Document.MIME_TYPE_DIR) {
                        val existingDocId = cursor.getString(idCol)
                        val uri = DocumentsContract.buildDocumentUriUsingTree(treeUri, existingDocId).toString()
                        return Pair(uri, existingDocId)
                    }
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "Error querying SAF child directory", e)
        }

        val created = try {
            DocumentsContract.createDocument(contentResolver, directoryUri, DocumentsContract.Document.MIME_TYPE_DIR, dirName)
        } catch (e: Exception) {
            Log.w(TAG, "Error creating SAF directory", e)
            null
        }

        val dirDocId = created?.let { DocumentsContract.getDocumentId(it) } ?: "$documentId/$dirName"
        val subDirUri = DocumentsContract.buildDocumentUriUsingTree(treeUri, dirDocId).toString()
        return Pair(subDirUri, dirDocId)
    }

    fun getOrCreateChildDocument(contentResolver: ContentResolver, pathString: String, dirDocId: String, fileName: String, mimeType: String = "video/mp2t"): String {
        if (!isContentUri(pathString)) {
            val file = File(dirDocId, fileName)
            file.parentFile?.mkdirs()
            return file.absolutePath
        }

        val treeUri = pathString.toUri()
        val subDirUri = DocumentsContract.buildDocumentUriUsingTree(treeUri, dirDocId)

        val childrenUri = DocumentsContract.buildChildDocumentsUriUsingTree(treeUri, dirDocId)
        val projection = arrayOf(
            DocumentsContract.Document.COLUMN_DOCUMENT_ID,
            DocumentsContract.Document.COLUMN_DISPLAY_NAME
        )
        try {
            contentResolver.query(childrenUri, projection, null, null, null)?.use { cursor ->
                val idCol = cursor.getColumnIndexOrThrow(DocumentsContract.Document.COLUMN_DOCUMENT_ID)
                val nameCol = cursor.getColumnIndexOrThrow(DocumentsContract.Document.COLUMN_DISPLAY_NAME)
                while (cursor.moveToNext()) {
                    if (cursor.getString(nameCol) == fileName) {
                        val existingDocId = cursor.getString(idCol)
                        return DocumentsContract.buildDocumentUriUsingTree(treeUri, existingDocId).toString()
                    }
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "Error querying child document in sub-dir", e)
        }

        val created = try {
            DocumentsContract.createDocument(contentResolver, subDirUri, mimeType, fileName)
        } catch (e: Exception) {
            Log.w(TAG, "Error creating child document in sub-dir", e)
            null
        }

        if (created != null) {
            return created.toString()
        }

        return (subDirUri.toString() + (if (!subDirUri.toString().endsWith("%3A")) "%2F" else "") + fileName)
    }

    fun truncateFile(contentResolver: ContentResolver, fileUriString: String, targetLength: Long) {
        if (targetLength < 0L) return
        try {
            if (isContentUri(fileUriString)) {
                contentResolver.openFileDescriptor(fileUriString.toUri(), "rw")?.use { pfd ->
                    FileOutputStream(pfd.fileDescriptor).channel.truncate(targetLength)
                }
            } else {
                val file = File(fileUriString)
                if (file.exists()) {
                    RandomAccessFile(file, "rw").use { raf ->
                        raf.setLength(targetLength)
                    }
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "Error truncating file $fileUriString to $targetLength", e)
        }
    }

    fun getFileSize(contentResolver: ContentResolver, fileUriString: String): Long {
        return try {
            if (isContentUri(fileUriString)) {
                contentResolver.openFileDescriptor(fileUriString.toUri(), "r")?.use { pfd ->
                    pfd.statSize
                } ?: -1L
            } else {
                val file = File(fileUriString)
                if (file.exists()) file.length() else -1L
            }
        } catch (e: Exception) {
            -1L
        }
    }

    fun openOutputStream(contentResolver: ContentResolver, fileUriString: String, append: Boolean = true): OutputStream {
        if (isContentUri(fileUriString)) {
            val mode = if (append) "wa" else "rwt"
            return contentResolver.openOutputStream(fileUriString.toUri(), mode)
                ?: contentResolver.openOutputStream(fileUriString.toUri(), "w")
                ?: throw java.io.IOException("Unable to open output stream for SAF URI: $fileUriString")
        } else {
            val file = File(fileUriString)
            file.parentFile?.mkdirs()
            return FileOutputStream(file, append)
        }
    }

    fun openInputStream(contentResolver: ContentResolver, fileUriString: String): InputStream {
        if (isContentUri(fileUriString)) {
            return contentResolver.openInputStream(fileUriString.toUri())
                ?: throw java.io.IOException("Unable to open input stream for SAF URI: $fileUriString")
        } else {
            return FileInputStream(File(fileUriString))
        }
    }

    fun fileExists(contentResolver: ContentResolver, fileUriString: String): Boolean {
        return try {
            if (isContentUri(fileUriString)) {
                contentResolver.openFileDescriptor(fileUriString.toUri(), "r")?.use { true } ?: false
            } else {
                File(fileUriString).exists()
            }
        } catch (e: Exception) {
            false
        }
    }
}
