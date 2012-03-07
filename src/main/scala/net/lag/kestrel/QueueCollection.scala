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
import java.nio.ByteBuffer
import java.util.concurrent.{CountDownLatch, ScheduledExecutorService}
import scala.collection.mutable
import com.twitter.conversions.time._
import com.twitter.libkestrel._
import com.twitter.libkestrel.config._
import com.twitter.logging.Logger
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Duration, Future, Time, Timer}
import config._

class InaccessibleQueuePath extends Exception("Inaccessible queue path: Must be a directory and writable")

class QueueCollection(
  queueFolder: String,
  timer: Timer,
  journalSyncScheduler: ScheduledExecutorService,
  defaultQueueBuilder: QueueBuilder,
  queueBuilders: Seq[QueueBuilder]
) {
  private val log = Logger.get(getClass.getName)

  private val path = new File(queueFolder)

  if (! path.isDirectory) {
    path.mkdirs()
  }
  if (! path.isDirectory || ! path.canWrite) {
    throw new InaccessibleQueuePath
  }

  private[this] val queueBuilderMap = queueBuilders.map { b => (b.name.value, b) }.toMap
  private[this] val queues = new mutable.HashMap[String, JournaledQueue]
  private[this] val seenReaders = new ConcurrentHashMap[String, JournaledQueue#Reader]
  @volatile private var shuttingDown = false

  private def buildQueue(name: String, realName: String, path: String) = {
    if ((name contains ".") || (name contains "/") || (name contains "~")) {
      throw new Exception("Queue name contains illegal characters (one of: ~ . /).")
    }
    val builder = queueBuilderMap.getOrElse(name, defaultQueueBuilder)
    val config = builder().copy(name = name)
    log.info("Setting up queue %s: %s", realName, config)
    Stats.incr("queue_creates")
    config.readersToStrings().foreach { s =>
      log.info("Queue %s reader %s", realName, s)
    }
    new JournaledQueue(config, new File(path), timer, journalSyncScheduler)
  }

  // preload any queues
  def loadQueues() {
    Journal.getQueueNamesFromFolder(path) foreach { name =>
      val w = writer(name)
      w foreach { _.readers foreach { r => reader(r.fullname) } }
    }
  }

  def queueNames: List[String] = synchronized {
    queues.keys.toList
  }

  def currentItems = queues.values.foldLeft(0L) { _ + _.items }
  def currentBytes = queues.values.foldLeft(0L) { _ + _.bytes }
  def reservedMemoryRatio = {
    val maxBytes = queues.values.foldLeft(0L) { _ + _.readers.foldLeft(0L) { _ + _.readerConfig.maxMemorySize.inBytes } }
    maxBytes.toDouble / systemMaxHeapBytes.toDouble
  }
  lazy val systemMaxHeapBytes = Runtime.getRuntime.maxMemory

  /**
   * Get a named queue, creating it if necessary.
   */
  def writer(name: String): Option[JournaledQueue] = synchronized {
    if (shuttingDown) {
      None
    } else {
      Some(queues.get(name) getOrElse {
        // only happens when creating a queue for the first time.
        val q = buildQueue(name, name, path.getPath)
        Stats.addGauge("q/" + name + "/journal_size")(q.journalBytes)
        queues(name) = q
        q
      })
    }
  }

  def reader(name: String): Option[JournaledQueue#Reader] = {
    val (writerName, readerName) = if (name contains '+') {
      val names = name.split("\\+", 2)
      (names(0), names(1))
    } else {
      (name, "")
    }
    val rv = writer(writerName).map { _.reader(readerName) }
    rv foreach { reader =>
      if (seenReaders.putIfAbsent(reader.fullname, reader) eq null) {
        val prefix = "q/" + reader.fullname + "/"
        Stats.makeCounter(prefix + "total_items", reader.putCount)
        Stats.makeCounter(prefix + "expired_items", reader.expiredCount)
        Stats.makeCounter(prefix + "discarded", reader.discardedCount)
        Stats.makeCounter(prefix + "total_flushes", reader.flushCount)
        Stats.addGauge(prefix + "items")(reader.items)
        Stats.addGauge(prefix + "bytes")(reader.bytes)
        Stats.addGauge(prefix + "mem_items")(reader.memoryItems)
        Stats.addGauge(prefix + "mem_bytes")(reader.memoryBytes)
        Stats.addGauge(prefix + "age_msec")(reader.age.inMilliseconds)
        Stats.addGauge(prefix + "open_transactions")(reader.openItems)
        Stats.addGauge(prefix + "waiters")(reader.waiterCount)
      }
    }
    rv
  }

  // Remove various stats related to the queue
  def removeStats(name: String) {
    val prefix = "q/" + name + "/"
    Stats.removeCounter(prefix + "total_items")
    Stats.removeCounter(prefix + "expired_items")
    Stats.removeCounter(prefix + "discarded")
    Stats.removeCounter(prefix + "total_flushes")
    Stats.clearGauge(prefix + "items")
    Stats.clearGauge(prefix + "bytes")
    Stats.clearGauge(prefix + "journal_size")
    Stats.clearGauge(prefix + "mem_items")
    Stats.clearGauge(prefix + "mem_bytes")
    Stats.clearGauge(prefix + "age_msec")
    Stats.clearGauge(prefix + "waiters")
    Stats.clearGauge(prefix + "open_transactions")
    Stats.removeMetric(prefix + "set_latency_usec")
    Stats.removeMetric(prefix + "delivery_latency_msec")
    Stats.removeMetric(prefix + "get_timeout_msec")
  }

  /**
   * Add an item to a named queue. Will not return until the item has been synchronously added
   * and written to the queue journal file.
   *
   * @return true if the item was added; false if the server is shutting down
   */
  def add(key: String, data: ByteBuffer, expiry: Option[Time], addTime: Time): Boolean = {
    writer(key) flatMap { q =>
      q.put(data, addTime, expiry) map { future =>
        future map { _ => Stats.incr("total_items") }
        true
      }
    } getOrElse(false)
  }

  def add(key: String, item: ByteBuffer): Boolean = add(key, item, None, Time.now)
  def add(key: String, item: ByteBuffer, expiry: Option[Time]): Boolean = add(key, item, expiry, Time.now)

  /**
   * Retrieve an item from a queue and pass it to a continuation. If no item is available within
   * the requested time, or the server is shutting down, None is passed.
   */
  def remove(key: String, deadline: Option[Time] = None, transaction: Boolean = false, peek: Boolean = false): Future[Option[QueueItem]] = {
    reader(key) match {
      case None =>
        Future.value(None)
      case Some(q) =>
        val future = if (peek) {
          q.peek(deadline)
        } else {
          q.get(deadline)
        }
        future.map { itemOption =>
          itemOption match {
            case None => {
              Stats.incr("get_misses")
            }
            case Some(item) => {
              Stats.incr("get_hits")
              if (!transaction && !peek) q.commit(item.id)
            }
          }
          itemOption
        }
    }
  }

  def unremove(key: String, xid: Long) {
    reader(key) map { q => q.unget(xid) }
  }

  def confirmRemove(key: String, xid: Long) {
    reader(key) map { q => q.commit(xid) }
  }

  def flush(key: String) {
    reader(key) map { q => q.flush()() }
  }

  def delete(name: String) {
    synchronized {
      if (!shuttingDown) {
        queues.get(name) foreach { q =>
          q.erase()
          queues -= name
          removeStats(name)
          Stats.incr("queue_deletes")
        }
        if (name contains '+') {
          reader(name) foreach { n => removeStats(n.fullname) }
          val (writerName, readerName) = {
            val names = name.split("\\+", 2)
            (names(0), names(1))
          }
          queues(writerName).dropReader(readerName)
        }
      }
    }
  }

  def flushExpired(name: String) {
    if (!shuttingDown) {
      writer(name) foreach { _.discardExpired() }
    }
  }

  def flushAllExpired() {
    queueNames foreach { queueName => flushExpired(queueName) }
  }

  def expireQueue(name: String): Unit = {
    if (!shuttingDown) {
      queues.get(name) map { q =>
      }
    }
  }

  def deleteExpiredQueues() {
    synchronized {
      if (shuttingDown) return
      queueNames foreach { name =>
        queues.get(name) foreach { q =>
          q.readers foreach { reader =>
            if (reader.isReadyForExpiration) {
              // if this is the default reader, fullname will be the whole queue.
              log.info("Expiring queue: %s", reader.fullname)
              delete(reader.fullname)
              Stats.incr("queue_expires")
            }
          }
        }
      }
    }
  }

  def stats(key: String): Array[(String, String)] = reader(key) match {
    case None => Array[(String, String)]()
    case Some(q) => {
      Array(
        ("items", (q.items - q.openItems).toString),
        ("bytes", (q.bytes - q.openBytes).toString),
        ("total_items", q.putCount.toString),
        ("logsize", q.writer.journalBytes.toString),
        ("expired_items", q.expiredCount.toString),
        ("mem_items", (q.memoryItems - q.openItems).toString),
        ("mem_bytes", (q.memoryBytes - q.openBytes).toString),
        ("age", q.age.inMilliseconds.toString),
        ("discarded", q.discardedCount.toString),
        ("waiters", q.waiterCount.toString),
        ("open_transactions", q.openItems.toString),
        ("total_flushes", q.flushCount.toString),
        ("create_time", q.createTime.inSeconds.toString)
      )
    }
  }

  def debugLog(queueName: String) {
    reader(queueName) foreach { reader =>
      log.info("%s: items=%d bytes=%d mem_items=%d mem_bytes=%d age=%s waiters=%d journal_size=%d",
        queueName,
        reader.items, reader.bytes, reader.memoryItems, reader.memoryBytes, reader.age,
        reader.waiterCount, reader.writer.journalBytes)
    }
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
      try {
        q.close()
      } catch {
        case e: Throwable => log.error(e, "Exception closing queue %s", name)
      }
    }
    queues.clear
  }
}
