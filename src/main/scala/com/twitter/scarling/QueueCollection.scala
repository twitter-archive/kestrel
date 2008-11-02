package com.twitter.scarling

import java.io.File
import scala.collection.mutable
import net.lag.configgy.{Config, ConfigMap}
import net.lag.logging.Logger


class InaccessibleQueuePath extends Exception("Inaccessible queue path")


class QueueCollection(private val queueFolder: String, private var queueConfigs: ConfigMap) {
  private val log = Logger.get

  private val path = new File(queueFolder)
  if (! path.isDirectory || ! path.canWrite) {
      throw new InaccessibleQueuePath
  }

  private val queues = new mutable.HashMap[String, PersistentQueue]
  private var shuttingDown = false

  // total of all data in all queues
  private var _currentBytes: Long = 0

  // total of all items in all queues
  private var _currentItems: Long = 0

  // total items added since the server started up.
  private var _totalAdded: Long = 0

  // hits/misses on removing items from the queue
  private var _queueHits: Long = 0
  private var _queueMisses: Long = 0

  // reader accessors:
  def currentBytes: Long = _currentBytes
  def currentItems: Long = _currentItems
  def totalAdded: Long = _totalAdded
  def queueHits: Long = _queueHits
  def queueMisses: Long = _queueMisses

  queueConfigs.subscribe { c =>
    synchronized {
      queueConfigs = c.getOrElse(new Config)
    }
  }


  def queueNames: List[String] = synchronized {
    queues.keys.toList
  }

  /**
   * Get a named queue, creating it if necessary.
   * Exposed only to unit tests.
   */
  private[scarling] def queue(name: String): Option[PersistentQueue] = {
    var setup = false
    var queue: Option[PersistentQueue] = None

    synchronized {
      if (shuttingDown) {
        return None
      }

      queue = queues.get(name) match {
        case q @ Some(_) => q
        case None =>
          setup = true
          val q = new PersistentQueue(path.getPath, name, queueConfigs.configMap(name))
          queues(name) = q
          Some(q)
      }
    }

    if (setup) {
      /* race is handled by having PersistentQueue start up with an
       * un-initialized flag that blocks all operations until this
       * method is called and completed:
       */
      queue.get.setup
      synchronized {
        _currentBytes += queue.get.bytes
        _currentItems += queue.get.length
      }
    }
    queue
  }

  /**
   * Add an item to a named queue. Will not return until the item has been
   * synchronously added and written to the queue journal file.
   *
   * @return true if the item was added; false if the server is shutting
   *     down
   */
  def add(key: String, item: Array[Byte], expiry: Int): Boolean = {
    queue(key) match {
      case None => false
      case Some(q) =>
        val now = System.currentTimeMillis
        val normalizedExpiry: Long = if (expiry == 0) {
          0
        } else if (expiry < 1000000) {
          now + expiry * 1000
        } else {
          expiry * 1000
        }
        val result = q.add(item, normalizedExpiry)
        if (result) {
          synchronized {
            _currentBytes += item.length
            _currentItems += 1
            _totalAdded += 1
          }
        }
        result
    }
  }

  def add(key: String, item: Array[Byte]): Boolean = add(key, item, 0)

  /**
   * Retrieve an item from a queue. If no item is available, or the server
   * is shutting down, None is returned.
   */
  def remove(key: String): Option[Array[Byte]] = {
    queue(key) match {
      case None =>
        synchronized { _queueMisses += 1 }
        None
      case Some(q) =>
        val item = q.remove
        synchronized {
          item match {
            case None => _queueMisses += 1
            case Some(x) =>
              _queueHits += 1
              _currentBytes -= x.length
              _currentItems -= 1
          }
        }
        item
    }
  }

  case class Stats(length: Long, bytes: Long, totalItems: Long, journalSize: Long,
                   totalExpired: Long, currentAge: Long)

  def stats(key: String): Stats = {
    queue(key) match {
      case None => Stats(0, 0, 0, 0, 0, 0)
      case Some(q) => Stats(q.length, q.bytes, q.totalItems, q.journalSize, q.totalExpired, q.currentAge)
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
