/*
 * Copyright 2009 Twitter, Inc.
 * Copyright 2009 Robey Pointer <robeypointer@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lag

import _root_.java.io.File
import _root_.java.util.UUID
import _root_.net.lag.logging.Logger
import _root_.org.specs.Specification


trait TestHelper { self: Specification =>
  private val _folderName = new ThreadLocal[File]

  new SpecContext {
    beforeSpec {
      Logger.get("").setLevel(Logger.FATAL)
    }
  }

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
      folder = new File(tempFolder, "scala-test-" + UUID.randomUUID.toString)
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

  def waitUntil(outcome: => Boolean): Boolean = { waitUntil(40, 100)(outcome) }

  def waitUntil(retries: Int, sleep: Long)(outcome: => Boolean): Boolean = {
    if (retries == 0) {
      false
    } else {
      outcome || {
        Thread.sleep(sleep)
        waitUntil(retries - 1, sleep)(outcome)
      }
    }
  }
}
