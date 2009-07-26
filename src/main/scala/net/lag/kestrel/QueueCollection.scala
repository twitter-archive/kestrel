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

package net.lag.kestrel

import java.io.File
import java.util.concurrent.CountDownLatch
import scala.collection.mutable
import net.lag.configgy.{Config, ConfigMap}
import net.lag.logging.Logger


class InaccessibleQueuePath extends Exception("Inaccessible queue path: Must be a directory and writable")


class QueueCollection(queueFolder: String, private var queueConfigs: ConfigMap) {
  private val log = Logger.get

  private val path = new File(queueFolder)

  if (! path.isDirectory) {
    path.mkdirs()
  }
  if (! path.isDirectory || ! path.canWrite) {
    throw new InaccessibleQueuePath
  }

  private val queues = new mutable.HashMap[String, PersistentQueue]
  private val fanout_queues = new mutable.HashMap[String, mutable.HashSet[String]]
  private var shuttingDown = false

  // total of all data in all queues
  val currentBytes = new Counter()

  // total of all items in all queues
  val currentItems = new Counter()

  // total items added since the server started up.
  val totalAdded = new Counter()

  // hits/misses on removing items from the queue
  val queueHits = new Counter()
  val queueMisses = new Counter()

  queueConfigs.subscribe { c =>
    synchronized {
      queueConfigs = c.getOrElse(new Config)
    }
  }

  // preload any queues
  def loadQueues() {
    path.list() map { queue(_) }
  }

  def queueNames: List[String] = synchronized {
    queues.keys.toList
  }

  /**
   * Get a named queue, creating it if necessary.
   * Exposed only to unit tests.
   */
  private[kestrel] def queue(name: String): Option[PersistentQueue] = synchronized {
    if (shuttingDown) {
      None
    } else {
      Some(queues.get(name) getOrElse {
        // only happens when creating a queue for the first time.
        val q = if (name contains '+') {
          val master = name.split('+')(0)
          fanout_queues.getOrElseUpdate(master, new mutable.HashSet[String]) += name
          log.info("Fanout queue %s added to %s", name, master)
          new PersistentQueue(path.getPath, name, queueConfigs.configMap(master))
        } else {
          new PersistentQueue(path.getPath, name, queueConfigs.configMap(name))
        }
        q.setup
        currentBytes.incr(q.bytes)
        currentItems.incr(q.length)
        queues(name) = q
        q
      })
    }
  }

  /**
   * Add an item to a named queue. Will not return until the item has been
   * synchronously added and written to the queue journal file.
   *
   * @return true if the item was added; false if the server is shutting
   *     down
   */
  def add(key: String, item: Array[Byte], expiry: Int): Boolean = {
    for (fanouts <- fanout_queues.get(key); name <- fanouts) {
      add(name, item, expiry)
    }

    queue(key) match {
      case None => false
      case Some(q) =>
        val now = Time.now
        val normalizedExpiry: Long = if (expiry == 0) {
          0
        } else if (expiry < 1000000) {
          now + expiry * 1000
        } else {
          expiry * 1000
        }
        val result = q.add(item, normalizedExpiry)
        if (result) {
          currentBytes.incr(item.length)
          currentItems.incr()
          totalAdded.incr()
        }
        result
    }
  }

  def add(key: String, item: Array[Byte]): Boolean = add(key, item, 0)

  /**
   * Retrieve an item from a queue and pass it to a continuation. If no item is available within
   * the requested time, or the server is shutting down, None is passed.
   */
  def remove(key: String, timeout: Int, transaction: Boolean, peek: Boolean)(f: Option[QItem] => Unit): Unit = {
    queue(key) match {
      case None =>
        queueMisses.incr
        f(None)
      case Some(q) =>
        if (peek) {
          f(q.peek())
        } else {
          q.removeReact(if (timeout == 0) timeout else Time.now + timeout, transaction) {
            case None =>
              queueMisses.incr
              f(None)
            case Some(item) =>
              queueHits.incr
              currentBytes.decr(item.data.length)
              currentItems.decr
              f(Some(item))
          }
        }
    }
  }

  // for testing.
  def receive(key: String): Option[Array[Byte]] = {
    var rv: Option[Array[Byte]] = None
    val latch = new CountDownLatch(1)
    remove(key, 0, false, false) {
      case None =>
        rv = None
        latch.countDown
      case Some(v) =>
        rv = Some(v.data)
        latch.countDown
    }
    latch.await
    rv
  }

  def unremove(key: String, xid: Int) {
    queue(key) map { q => q.unremove(xid) }
  }

  def confirmRemove(key: String, xid: Int) {
    queue(key) map { q => q.confirmRemove(xid) }
  }

  def flush(key: String) {
    queue(key) map { q => q.flush() }
  }

  def delete(name: String): Unit = synchronized {
    if (!shuttingDown) {
      queues.get(name) map { q =>
        q.close()
        q.destroyJournal()
        queues.removeKey(name)
      }
      if (name contains '+') {
        val master = name.split('+')(0)
        fanout_queues.getOrElseUpdate(master, new mutable.HashSet[String]) -= name
        log.info("Fanout queue %s dropped from %s", name, master)
      }
    }
  }

  def flushExpired(name: String): Int = synchronized {
    if (shuttingDown) {
      0
    } else {
      queue(name) map { q => q.discardExpired() } getOrElse(0)
    }
  }

  case class Stats(items: Long, bytes: Long, totalItems: Long, journalSize: Long,
                   totalExpired: Long, currentAge: Long, memoryItems: Long, memoryBytes: Long,
                   totalDiscarded: Long)

  def stats(key: String): Stats = {
    queue(key) match {
      case None => Stats(0, 0, 0, 0, 0, 0, 0, 0, 0)
      case Some(q) => Stats(q.length, q.bytes, q.totalItems, q.journalSize,
                            q.totalExpired, q.currentAge, q.memoryLength, q.memoryBytes,
                            q.totalDiscarded)
    }
  }

  def dumpConfig(key: String): Array[String] = {
    queue(key) match {
      case None => Array()
      case Some(q) => q.dumpConfig()
    }
  }

  /**
   * Shutdown this queue collection. All actors are asked to exit, and
   * any future queue requests will fail.
   */
  def shutdown: Unit = synchronized {
    if (shuttingDown) {
      return
    }
    shuttingDown = true
    for ((name, q) <- queues) {
      // synchronous, so the journals are all officially closed before we return.
      q.close
    }
    queues.clear
  }
}
