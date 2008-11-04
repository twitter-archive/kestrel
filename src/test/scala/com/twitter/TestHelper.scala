/** Copyright 2008 Twitter, Inc. */
package com.twitter

import java.io.File


trait TestHelper {
  private val _folderName = new ThreadLocal[File]

  /**
   * Recursively delete a folder. Should be built in; bad java.
   */
  def deleteFolder(folder: File): Unit = {
    val files = folder.listFiles
    if (files != null) {
      for (val f <- files) {
        if (f.isDirectory) {
          deleteFolder(f)
        } else {
          f.delete
        }
      }
    }
    folder.delete
  }

  def withTempFolder(f: => Any): Unit = {
    val tempFolder = System.getProperty("java.io.tmpdir")
    var folder: File = null
    do {
      folder = new File(tempFolder, "scala-test-" + System.currentTimeMillis)
    } while (! folder.mkdir)
    _folderName.set(folder)

    try {
      f
    } finally {
      deleteFolder(folder)
    }
  }

  def folderName = { _folderName.get.getPath }

  def canonicalFolderName = { _folderName.get.getCanonicalPath }
}
