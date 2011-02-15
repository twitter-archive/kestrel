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
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.stats.Stats
import com.twitter.util.{Duration, Time, Timer}
import config._

class InaccessibleQueuePath extends Exception("Inaccessible queue path: Must be a directory and writable")

class QueueCollection(queueFolder: String, timer: Timer,
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
  private var shuttingDown = false

  @volatile private var queueConfigMap = Map(queueBuilders.map { builder => (builder.name, builder()) }: _*)

  private def buildQueue(name: String, realName: String, path: String) = {
    val config = queueConfigMap.getOrElse(name, defaultQueueConfig)
    log.info("Setting up queue %s: %s", realName, config)
    new PersistentQueue(realName, path, config, timer, Some(this.apply))
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
  def add(key: String, item: Array[Byte], expiry: Option[Time]): Boolean = {
    for (fanouts <- fanout_queues.get(key); name <- fanouts) {
      add(name, item, expiry)
    }

    queue(key) match {
      case None => false
      case Some(q) =>
        val result = q.add(item, expiry)
        if (result) Stats.incr("total_items")
        result
    }
  }

  def add(key: String, item: Array[Byte]): Boolean = add(key, item, None)

  /**
   * Retrieve an item from a queue and pass it to a continuation. If no item is available within
   * the requested time, or the server is shutting down, None is passed.
   */
  def remove(key: String, deadline: Option[Time], transaction: Boolean, peek: Boolean)(f: Option[QItem] => Unit): Unit = {
    queue(key) match {
      case None =>
        Stats.incr("get_misses")
        f(None)
      case Some(q) =>
        if (peek) {
          f(q.peek())
        } else {
          q.waitRemove(deadline, transaction) { item =>
            item match {
              case None =>
                Stats.incr("get_misses")
                f(None)
              case Some(item) =>
                Stats.incr("get_hits")
                f(Some(item))
            }
          }
        }
    }
  }

  // for testing.
  def receive(key: String): Option[Array[Byte]] = {
    var rv: Option[Array[Byte]] = None
    val latch = new CountDownLatch(1)
    remove(key, None, false, false) {
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
        queues.remove(name)
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
      queue(name) map { q => q.discardExpired(q.config.maxExpireSweep) } getOrElse(0)
    }
  }

  def flushAllExpired(): Int = synchronized {
    queueNames.foldLeft(0) { (sum, qName) => sum + flushExpired(qName) }
  }

  def rollJournal(name: String) {
    if (!shuttingDown) {
      queue(name).foreach { _.rollJournal() }
    }
  }

  def stats(key: String): Array[(String, String)] = queue(key) match {
    case None => Array[(String, String)]()
    case Some(q) =>
      q.dumpStats() ++
        fanout_queues.get(key).map { qset => ("children", qset.mkString(",")) }.toList
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
