/*
 * Copyright (c) 2008 Robey Pointer <robeypointer@lag.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package net.lag

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
