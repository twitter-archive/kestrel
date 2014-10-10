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

import java.nio.ByteBuffer
import com.twitter.util.{Future, Duration, Time}

/** Abstractions for the interface used by Kestrel to persists
 * contents in its Journal
 *
 *    +-------------+
 *    |  Queue      |
 *    |             |
 *    +------+------+
 *           |
 *           |1:1
 *           |
 *    +------V------+
 *    |  Journal    |
 *    |             |
 *    +------+------+
 *           |
 *           |1:N
 *           |
 *    +------V------+
 *    | Persistent  |
 *    |   Stream    |
 *    +-------------+
 */

class ContainerInitException(msg: String) extends Exception(msg)

object StreamContainerFactory {
  def apply(className: String): StreamContainerFactory = {
    try {
      Class.forName(className).newInstance.asInstanceOf[StreamContainerFactory]
    } catch { case ex: ClassNotFoundException => 
      throw new ContainerInitException("Cannot init StreamContainerFactory: invalid class name %s".format(className))
    }
  }
}

abstract class StreamContainerFactory {
  def createStreamContainer(): PersistentStreamContainer
  def createStatusStore(): PersistentMetadataStore
}

/*
 * The PersistentStreamContainer provides methods to create new journals
 * (each of which is a persistent stream) within a queue or new queues
 * It also provides mechanism to enumerate all the existing streams - this is
 * used to discover queues during restart (recovery)
 */
abstract class PersistentStreamContainer {
  def getStream(streamName: String, syncPeriod: Duration): PersistentStream
  def renameStream(oldName: String, newName: String)
  def deleteStream(streamName: String)
  def uniqueStreamName(prefix: String, infix: String, suffix: String = ""): String
  def listStreams: Array[String]
  def listQueues: Set[String]
}

/**
 * PersistentStream is an append only stream within a journal.
 * Journals can contain multiple streams as they are rotated
 * Each of these streams has a name associated with it which
 * helps Kestrel tie it to a specific queue
 */
trait PersistentStream {
  def getWriter: PersistentStreamWriter
  def getReader: PersistentStreamReader
  def length: Long
  def recover()
}

/*
 * Interface to read from a persistent stream
 */
trait PersistentStreamReader {
  def read(dataBuffer: ByteBuffer): Int
  def positionAt(newPosition: Long)
  def position: Long
  def close()
}

/*
 * Interface to write to a persistent stream
 */
trait PersistentStreamWriter {
  def write(data: ByteBuffer): Future[Unit]
  def force(metadata: Boolean)
  def truncate(position: Long)
  def position: Long
  def close()
}

abstract class PersistentMetadataStore {
  def storeMetadata(metadata: String)
  def readMetadata: String
  def exists: Boolean
}