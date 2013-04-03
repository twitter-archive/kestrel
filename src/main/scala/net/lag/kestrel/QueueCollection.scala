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

object QueueCollection {
  val unknown = () => "<unknown>"
}

class QueueCollection(queueFolder: String, timer: Timer, journalSyncScheduler: ScheduledExecutorService,
                      @volatile private var defaultQueueConfig: QueueConfig,
                      @volatile var queueBuilders: List[QueueBuilder],
                      @volatile var aliasBuilders: List[AliasBuilder]) {
  import QueueCollection.unknown
  type SessionDescription = Option[() => String]

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
  private val aliases = new mutable.HashMap[String, AliasedQueue]
  @volatile private var shuttingDown = false

  @volatile private var queueBuilderMap = Map(queueBuilders.map { builder => (builder.name, builder) }: _*)
  @volatile private var aliasConfigMap = Map(aliasBuilders.map { builder => (builder.name, builder()) }: _*)

  private def checkNames {
    val duplicates = queueBuilderMap.keySet & aliasConfigMap.keySet
    if (!duplicates.isEmpty) {
      log.warning("queue name(s) masked by alias(es): %s".format(duplicates.toList.sorted.mkString(", ")))
    }
  }

  private def getQueueConfig(name: String, masterName: Option[String] = None): QueueConfig = {
    masterName match {
      case Some(master) =>
        val masterConfig = getQueueConfig(master)
        queueBuilderMap.get(name).map { _.apply(Some(masterConfig)) }.getOrElse(masterConfig)
      case None =>
        queueBuilderMap.get(name).map { _.apply(Some(defaultQueueConfig)) }.getOrElse(defaultQueueConfig)
    }
  }

  private def buildQueue(name: String, masterName: Option[String], path: String,
                         sessionDescription: SessionDescription) = {
    if ((name contains ".") || (name contains "/") || (name contains "~")) {
      throw new Exception("Queue name contains illegal characters (one of: ~ . /).")
    }
    val config = getQueueConfig(name, masterName)
    log.info("Setting up queue %s: %s (via %s)", name, config, sessionDescription.getOrElse(unknown)())
    Stats.incr("queue_creates")
    new PersistentQueue(name, path, config, timer, journalSyncScheduler, Some(this.apply))
  }

  // preload any queues
  def loadQueues() {
    val startupDesc = Some(() => "<startup>")
    Journal.getQueueNamesFromFolder(path) map { queue(_, startupDesc) }
    createAliases()
  }

  def createAliases(): Unit = synchronized {
    checkNames
    aliasConfigMap.foreach { case (name, config) =>
      aliases.get(name) match {
        case Some(alias) =>
          alias.config = config
          log.info("Reloaded alias config %s: %s", name, config)
        case None =>
          log.info("Setting up alias %s: %s", name, config)
          val alias = new AliasedQueue(name, config, this)
          aliases(name) = alias
      }
    }
  }

  def queueNames(excludeAliases: Boolean): List[String] = {
    val names = synchronized {
      if (excludeAliases) {
        queues.keys
      } else {
        queues.keys ++ aliases.keys
      }
    }
    names.toList
  }

  def queueNames: List[String] = queueNames(false)

  def currentItems = queues.values.foldLeft(0L) { _ + _.length }
  def currentBytes = queues.values.foldLeft(0L) { _ + _.bytes }
  def reservedMemoryRatio = {
    val maxBytes = queues.values.foldLeft(0L) { _ + _.maxMemoryBytes }
    maxBytes.toDouble / systemMaxHeapBytes.toDouble
  }
  lazy val systemMaxHeapBytes = Runtime.getRuntime.maxMemory

  def reload(newDefaultQueueConfig: QueueConfig, newQueueBuilders: List[QueueBuilder],
             newAliasBuilders: List[AliasBuilder]) {
    defaultQueueConfig = newDefaultQueueConfig
    queueBuilders = newQueueBuilders
    queueBuilderMap = Map(queueBuilders.map { builder => (builder.name, builder) }: _*)
    queues.foreach { case (name, queue) =>
      val masterName = if (name contains '+') Some(name.split('+')(0)) else None
      val config = getQueueConfig(name, masterName)
      queue.config = config
      log.info("Reloaded queue config %s: %s", name, config)
    }
    aliasBuilders = newAliasBuilders
    aliasConfigMap = Map(aliasBuilders.map { builder => (builder.name, builder()) }: _*)
    createAliases()
  }

  /**
   * Get a named queue, creating it if necessary.
   */
  def queue(name: String, sessionDescription: SessionDescription = None): Option[PersistentQueue] =
    queue(name, true, sessionDescription)

  /**
   * Get a named queue, optionally creating it if it does not already exist.
   */
  def queue(name: String, create: Boolean): Option[PersistentQueue] =
    queue(name, create, None)

  /**
   * Get a named queue, with control over whether non-existent queues are created.
   */
  def queue(name: String, create: Boolean, sessionDescription: SessionDescription): Option[PersistentQueue] =
    synchronized {
      if (shuttingDown) {
        None
      } else if (create) {
        queues.get(name) orElse {
          // only happens when creating a queue for the first time.
          val q = if (name contains '+') {
            val master = name.split('+')(0)
            val fanoutQ = buildQueue(name, Some(master), path.getPath, sessionDescription)
            fanout_queues.getOrElseUpdate(master, new mutable.HashSet[String]) += name
            log.info("Fanout queue %s added to %s by %s", name, master, sessionDescription.getOrElse(unknown)())
            fanoutQ
          } else {
            buildQueue(name, None, path.getPath, sessionDescription)
          }
          q.setup
          queues(name) = q
          Some(q)
        }
      } else {
        queues.get(name)
      }
    }

  def apply(name: String) = queue(name)

  /**
   * Get an alias, creating it if necessary.
   */
  def alias(name: String): Option[AliasedQueue] = synchronized {
    if (shuttingDown) {
      None
    } else {
      aliases.get(name)
    }
  }

  /**
   * Add an item to a named queue. Will not return until the item has been synchronously added
   * and written to the queue journal file.
   *
   * @return true if the item was added; false if the server is shutting down
   */
  def add(key: String, item: Array[Byte], expiry: Option[Time], addTime: Time,
          sessionDescription: SessionDescription = None): Boolean = {
    alias(key) match {
      case Some(alias) =>
        alias.add(item, expiry, addTime, sessionDescription)
      case None =>
        for (fanouts <- fanout_queues.get(key); name <- fanouts) {
          add(name, item, expiry, addTime, sessionDescription)
        }

        queue(key, sessionDescription) match {
          case None => false
          case Some(q) =>
            val result = q.add(item, expiry, None, addTime)
          if (result) Stats.incr("total_items")
          result
        }
    }
  }

  def add(key: String, item: Array[Byte]): Boolean = add(key, item, None, Time.now)
  def add(key: String, item: Array[Byte], expiry: Option[Time]): Boolean = add(key, item, expiry, Time.now)

  /**
   * Retrieve an item from a queue and pass it to a continuation. If no item is available within
   * the requested time, or the server is shutting down, None is passed.
   */
  def remove(key: String, deadline: Option[Time] = None, transaction: Boolean = false, peek: Boolean = false,
           sessionDescription: SessionDescription = None): Future[Option[QItem]] = {
    if (alias(key).isDefined) {
      // make remove from alias return "no items"
      return Future.value(None)
    }

    queue(key, sessionDescription) match {
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
            case Some(qitem) =>
              Stats.incr("get_hits")
              if (!peek && transaction && q.shouldTraceQOps) {
                log.info("get -> q=%s session=%s xid=%d", key, sessionDescription.getOrElse(unknown), qitem.xid)
              }
          }
          item
        }
    }
  }

  def unremove(key: String, xid: Int) {
    queue(key, false) map { q =>
      q.unremove(xid)
      if (q.shouldTraceQOps) {
        log.info ("abort -> q=%s, xid=%d", key, xid)
      }
    }
  }

  def confirmRemove(key: String, xid: Int) {
    queue(key, false) map { q =>
      q.confirmRemove(xid)
      if (q.shouldTraceQOps) {
        log.info ("confirm -> q=%s, xid=%d", key, xid)
      }
    }
  }

  def flush(key: String, sessionDescription: SessionDescription = None) {
    queue(key, false) map { q =>
      q.flush()
      log.info("Queue %s flushed by %s", key, sessionDescription.getOrElse(unknown)())
    }
  }

  def delete(name: String, sessionDescription: SessionDescription = None): Unit = synchronized {
    if (!shuttingDown) {
      queues.get(name) map { q =>
        q.close()
        q.destroyJournal()
        q.removeStats()
        queues.remove(name)
        Stats.incr("queue_deletes")
        log.info("Queue %s deleted by %s", name, sessionDescription.getOrElse(unknown)())
      }
      if (name contains '+') {
        val master = name.split('+')(0)
        fanout_queues.getOrElseUpdate(master, new mutable.HashSet[String]) -= name
        log.info("Fanout queue %s dropped from %s by %s", name, master, sessionDescription.getOrElse(unknown)())
      }
    }
  }

  def flushExpired(name: String, limit: Boolean = false, sessionDescription: SessionDescription = None): Int = {
    if (shuttingDown) {
      0
    } else {
      queue(name, false) map { q =>
        val flushed = q.discardExpired(limit)
        if (flushed > 0) {
          log.info("Queue %s flushed of %d expired item(s) by %s",
                   name, flushed, sessionDescription.getOrElse(unknown)())
        }
        flushed
      } getOrElse(0)
    }
  }

  def expireQueue(name: String): Unit = synchronized {
    if (!shuttingDown) {
      queues.get(name) map { q =>
        if (q.isReadyForExpiration) {
          delete(name)
          Stats.incr("queue_expires")
          log.info("Expired queue %s", name)
        }
      }
    }
  }

  def flushAllExpired(limit: Boolean = false, sessionDescription: SessionDescription = None): Int = {
    queueNames(true).foldLeft(0) { (sum, qName) => sum + flushExpired(qName, limit, sessionDescription) }
  }

  def deleteExpiredQueues(): Unit = {
    queueNames(true).map { qName => expireQueue(qName) }
  }

  def stats(key: String): Array[(String, String)] = {
    queue(key, false) match {
      case Some(q) =>
        q.dumpStats() ++
          fanout_queues.get(key).map { qset => ("children", qset.mkString(",")) }.toList
      case None =>
        // check for alias under this name
        alias(key) match {
          case Some(a) => a.dumpStats()
          case None => Array[(String, String)]()
        }
    }
  }

  /**
   * Force any requests in a waiting opertion (e.g., remove) to react as
   * if canceled. This method is useful for forcing connections to drain
   * as part of shutting down.
   */
  def evictWaiters(): Unit = synchronized {
    if (shuttingDown) {
      return
    }

    for ((name, q) <- queues) {
      q.evictWaiters()
    }
  }

  /**
   * Shutdown this queue collection. Any future queue requests will fail.
   */
  def shutdown(): Unit = {
    shutdown(false)
  }

  /**
   * Shutdown this queue collection. Any future queue requests will fail.
   */
  def shutdown(gracefulShutdown: Boolean): Unit = synchronized {
    if (shuttingDown) {
      return
    }
    shuttingDown = true
    for ((name, q) <- queues) {
      // synchronous, so the journals are all officially closed before we return.
      q.close(gracefulShutdown)
    }
    queues.clear
  }
}
