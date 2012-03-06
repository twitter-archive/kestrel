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
import java.util.concurrent.{CountDownLatch, ScheduledExecutorService}
import scala.collection.mutable
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Duration, Future, Time, Timer}
import config._

class InaccessibleQueuePath extends Exception("Inaccessible queue path: Must be a directory and writable")

class QueueCollection(queueFolder: String, timer: Timer, journalSyncScheduler: ScheduledExecutorService,
                      @volatile private var defaultQueueConfig: QueueConfig,
                      @volatile var queueBuilders: List[QueueBuilder]) {
  private val log = Logger.get(getClass.getName)

  private val path = new File(queueFolder)

  if (! path.isDirectory) {
    path.mkdirs()
  }
  if (! path.isDirectory || ! path.canWrite) {
    throw new InaccessibleQueuePath
  }

  private val queues = new mutable.HashMap[String, PersistentQueue]
  private val fanout_queues = new mutable.HashMap[String, mutable.HashSet[String]]
  @volatile private var shuttingDown = false

  @volatile private var queueConfigMap = Map(queueBuilders.map { builder => (builder.name, builder()) }: _*)

  private def buildQueue(name: String, realName: String, path: String) = {
    if ((name contains ".") || (name contains "/") || (name contains "~")) {
      throw new Exception("Queue name contains illegal characters (one of: ~ . /).")
    }
    val config = queueConfigMap.getOrElse(name, defaultQueueConfig)
    log.info("Setting up queue %s: %s", realName, config)
    Stats.incr("queue_creates")
    new PersistentQueue(realName, path, config, timer, journalSyncScheduler, Some(this.apply))
  }

  // preload any queues
  def loadQueues() {
    Journal.getQueueNamesFromFolder(path) map { queue(_) }
  }

  def queueNames: List[String] = synchronized {
    queues.keys.toList
  }

  def currentItems = queues.values.foldLeft(0L) { _ + _.length }
  def currentBytes = queues.values.foldLeft(0L) { _ + _.bytes }
  def reservedMemoryRatio = {
    val maxBytes = queues.values.foldLeft(0L) { _ + _.maxMemoryBytes }
    maxBytes.toDouble / systemMaxHeapBytes.toDouble
  }
  lazy val systemMaxHeapBytes = Runtime.getRuntime.maxMemory

  def reload(newDefaultQueueConfig: QueueConfig, newQueueBuilders: List[QueueBuilder]) {
    defaultQueueConfig = newDefaultQueueConfig
    queueBuilders = newQueueBuilders
    queueConfigMap = Map(queueBuilders.map { builder => (builder.name, builder()) }: _*)
    queues.foreach { case (name, queue) =>
      val configName = if (name contains '+') name.split('+')(0) else name
      queue.config = queueConfigMap.get(configName).getOrElse(defaultQueueConfig)
    }
  }

  /**
   * Get a named queue, creating it if necessary.
   */
  def queue(name: String): Option[PersistentQueue] = synchronized {
    if (shuttingDown) {
      None
    } else {
      Some(queues.get(name) getOrElse {
        // only happens when creating a queue for the first time.
        val q = if (name contains '+') {
          val master = name.split('+')(0)
          fanout_queues.getOrElseUpdate(master, new mutable.HashSet[String]) += name
          log.info("Fanout queue %s added to %s", name, master)
          buildQueue(master, name, path.getPath)
        } else {
          buildQueue(name, name, path.getPath)
        }
        q.setup
        queues(name) = q
        q
      })
    }
  }

  def apply(name: String) = queue(name)

  /**
   * Add an item to a named queue. Will not return until the item has been synchronously added
   * and written to the queue journal file.
   *
   * @return true if the item was added; false if the server is shutting down
   */
  def add(key: String, item: Array[Byte], expiry: Option[Time], addTime: Time): Boolean = {
    for (fanouts <- fanout_queues.get(key); name <- fanouts) {
      add(name, item, expiry, addTime)
    }

    queue(key) match {
      case None => false
      case Some(q) =>
        val result = q.add(item, expiry, None, addTime)
        if (result) Stats.incr("total_items")
        result
    }
  }

  def add(key: String, item: Array[Byte]): Boolean = add(key, item, None, Time.now)
  def add(key: String, item: Array[Byte], expiry: Option[Time]): Boolean = add(key, item, expiry, Time.now)

  /**
   * Retrieve an item from a queue and pass it to a continuation. If no item is available within
   * the requested time, or the server is shutting down, None is passed.
   */
  def remove(key: String, deadline: Option[Time] = None, transaction: Boolean = false, peek: Boolean = false): Future[Option[QItem]] = {
    queue(key) match {
      case None =>
        Future.value(None)
      case Some(q) =>
        val future = if (peek) {
          q.waitPeek(deadline)
        } else {
          q.waitRemove(deadline, transaction)
        }
        future.map { item =>
          item match {
            case None =>
              Stats.incr("get_misses")
            case Some(_) =>
              Stats.incr("get_hits")
          }
          item
        }
    }
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
        q.removeStats()
        queues.remove(name)
        Stats.incr("queue_deletes")
      }
      if (name contains '+') {
        val master = name.split('+')(0)
        fanout_queues.getOrElseUpdate(master, new mutable.HashSet[String]) -= name
        log.info("Fanout queue %s dropped from %s", name, master)
      }
    }
  }

  def flushExpired(name: String): Int = {
    if (shuttingDown) {
      0
    } else {
      queue(name) map { q => q.discardExpired(q.config.maxExpireSweep) } getOrElse(0)
    }
  }
  
  def expireQueue(name: String): Unit = {
    if (!shuttingDown) {
      queues.get(name) map { q =>
        if (q.isReadyForExpiration) {
          delete(name)
          Stats.incr("queue_expires")
          log.info("Expired queue %s", name)
        }
      }
    }
    return 1
  }

  def flushAllExpired(): Int = {
    queueNames.foldLeft(0) { (sum, qName) => sum + flushExpired(qName) }
  }
  
  def deleteExpiredQueues(): Unit = {
    queueNames.map { qName => expireQueue(qName) }
  }

  def stats(key: String): Array[(String, String)] = queue(key) match {
    case None => Array[(String, String)]()
    case Some(q) =>
      q.dumpStats() ++
        fanout_queues.get(key).map { qset => ("children", qset.mkString(",")) }.toList
  }

  /**
   * Shutdown this queue collection. Any future queue requests will fail.
   */
  def shutdown(): Unit = synchronized {
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
