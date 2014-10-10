/*
 * Copyright 2013 Twitter, Inc.
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
package net.lag.kestrel

import com.twitter.util.Time
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.util.{Future, Duration, Time}
import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.ScheduledExecutorService


/**
 * Implementations of the PersistentStreamContainer and associated classes
 * to store data in local files
 */

class LocalFSContainerFactory (
  queuePath: String, 
  statusFile: String, 
  journalSyncScheduler: ScheduledExecutorService
) extends StreamContainerFactory {

  val path = new File(queuePath)
  if (!path.isDirectory) {
    path.mkdirs()
  }
  if (!path.isDirectory || !path.canWrite) {
    throw new InaccessibleQueuePath
  }

  def createStreamContainer(): PersistentStreamContainer = {
    new LocalDirectory(queuePath, journalSyncScheduler)
  }

  def createStatusStore(): PersistentMetadataStore = {
    new LocalMetadataStore(statusFile, "status")
  }
}

class LocalDirectory(path: String, syncScheduler: ScheduledExecutorService) extends PersistentStreamContainer {
  override def getStream(streamName: String, syncPeriod: Duration): PersistentStream = {
    new LocalFile(path, streamName, syncPeriod)
  }

  override def renameStream(oldName: String, newName: String) {
    new File(path, oldName).renameTo(new File(path, newName))
  }

  override def uniqueStreamName(prefix: String, infix: String, suffix: String) = {
    var file = new File(path, prefix + infix + Time.now.inMilliseconds + suffix)
    while (!file.createNewFile()) {
      Thread.sleep(1)
      file = new File(path, prefix + infix + Time.now.inMilliseconds + suffix)
    }
    file.getName
  }

  override def listStreams = {
    new File(path).list()
  }

  override def listQueues = {
    (new File(path)).listFiles().filter { file =>
      !file.isDirectory()
    }.map { file =>
      file.getName
    }.filter { name =>
      !(name contains "~~")
    }.map { name =>
      name.split('.')(0)
    }.toSet
  }

  override def deleteStream(streamName: String) {
    new File(path, streamName).delete()
  }

  private class LocalFile(file: File, syncPeriod: Duration) extends PersistentStream {
    def this (path: String, fileName: String, syncPeriod: Duration) = this(new File(path, fileName), syncPeriod)
    override def getWriter = new PeriodicSyncFileWriter(file, syncPeriod)
    override def getReader = new LocalFileReader(file)
    override def length: Long = file.length()
    override def recover() {}
  }

  private class LocalFileReader(file: File) extends PersistentStreamReader {
    val fileInputStream = new FileInputStream(file)
    val reader = fileInputStream.getChannel
    override def read(dataBuffer: ByteBuffer) = {
      reader.read(dataBuffer)
    }

    override def positionAt(newPosition: Long) {
      reader.position(newPosition)
    }

    override def position = reader.position()

    override def close() {
      reader.close()
      fileInputStream.close()
    }
  }

  private class PeriodicSyncFileWriter(file: File, syncPeriod: Duration) extends PersistentStreamWriter {
    val writer = new FileOutputStream(file, true).getChannel
    val syncStorage = new PeriodicSyncStorage(writer, syncScheduler, syncPeriod)
    override def write(data: ByteBuffer): Future[Unit] = {
      syncStorage.write(data)
    }

    override def force(metadata: Boolean) {
      syncStorage.fsync()
    }

    override def close() {
      // calls writer.close
      syncStorage.close()
    }

    override def position: Long = {
      syncStorage.position
    }

    override def truncate(position: Long) {
      writer.truncate(position)
    }
  }
}

class LocalMetadataStore(path: String, val statusLabel: String) extends PersistentMetadataStore {
  private val log = Logger.get(getClass.getName)
  private val metadataFile = new File(path)
  if (!metadataFile.getParentFile.isDirectory) metadataFile.getParentFile.mkdirs()

  override def exists: Boolean = metadataFile.exists()

  override def storeMetadata(metadata: String) {
    val writer = new OutputStreamWriter(new FileOutputStream(metadataFile), "UTF-8")
    try {
      writer.write(metadata + "\n")
      log.debug("stored %s '%s' in '%s'", statusLabel, metadata, metadataFile)
    } catch { case e =>
      log.error(e, "unable store %s at '%s'", statusLabel, metadataFile)
    } finally {
      writer.close()
    }
  }

  override def readMetadata: String = {
    val reader =
      new BufferedReader(new InputStreamReader(new FileInputStream(metadataFile), "UTF-8"))
    try {
      reader.readLine
    } finally {
      reader.close()
    }
  }

  override def toString = metadataFile.toString
}

