package com.twitter.scarling

import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel
import java.util.concurrent.CountDownLatch
import scala.actors.{Actor, TIMEOUT}
import scala.collection.mutable
import net.lag.configgy.{Config, Configgy, ConfigMap}
import net.lag.logging.Logger


case class QItem(addTime: Long, expiry: Long, data: Array[Byte], var xid: Int)


class PersistentQueue(private val persistencePath: String, val name: String,
                      val config: ConfigMap) {

  private case class Waiter(actor: Actor)
  private case object ItemArrived


  private val log = Logger.get

  // current size of all data in the queue:
  private var queueSize: Long = 0

  // # of items EVER added to the queue:
  private var _totalItems: Long = 0

  // # of items that were expired by the time they were read:
  private var _totalExpired: Long = 0

  // age (in milliseconds) of the last item read from the queue:
  private var _currentAge: Long = 0

  // # of items in the queue (including those not in memory)
  private var queueLength: Long = 0

  private var queue = new mutable.Queue[QItem] {
    // scala's Queue doesn't (yet?) have a way to put back.
    def unget(item: QItem) = prependElem(item)
  }
  private var _memoryBytes: Long = 0
  private var journal = new Journal(new File(persistencePath, name).getCanonicalPath)

  // force get/set operations to block while we're replaying any existing journal
  private val initialized = new CountDownLatch(1)
  private var closed = false

  // attempting to add an item after the queue reaches this size will fail.
  var maxItems = Math.MAX_INT

  // maximum expiration time for this queue (seconds).
  var maxAge = 0

  // clients waiting on an item in this queue
  private val waiters = new mutable.ArrayBuffer[Waiter]

  // track tentative removals
  private var xidCounter: Int = 0
  private val openTransactions = new mutable.HashMap[Int, QItem]

  def length: Long = synchronized { queueLength }

  def totalItems: Long = synchronized { _totalItems }

  def bytes: Long = synchronized { queueSize }

  def journalSize: Long = synchronized { journal.size }

  def totalExpired: Long = synchronized { _totalExpired }

  def currentAge: Long = synchronized { _currentAge }

  // mostly for unit tests.
  def memoryLength: Long = synchronized { queue.size }
  def memoryBytes: Long = synchronized { _memoryBytes }
  def inReadBehind = synchronized { journal.inReadBehind }


  config.subscribe(configure _)
  configure(Some(config))

  def configure(c: Option[ConfigMap]) = synchronized {
    for (config <- c) {
      maxItems = config("max_items", Math.MAX_INT)
      maxAge = config("max_age", 0)
    }
  }

  private final def adjustExpiry(expiry: Long): Long = {
    if (maxAge > 0) {
      if (expiry > 0) (expiry min maxAge) else maxAge
    } else {
      expiry
    }
  }

  /**
   * Add a value to the end of the queue, transactionally.
   */
  def add(value: Array[Byte], expiry: Long): Boolean = {
    initialized.await
    synchronized {
      if (closed || queueLength >= maxItems) {
        false
      } else {
        val item = QItem(System.currentTimeMillis, adjustExpiry(expiry), value, 0)
        if (!journal.inReadBehind && queueSize >= PersistentQueue.maxMemorySize) {
          log.info("Dropping to read-behind for queue '%s' (%d bytes)", name, queueSize)
          journal.startReadBehind
        }
        _add(item)
        journal.add(item)
        if (waiters.size > 0) {
          waiters.remove(0).actor ! ItemArrived
        }
        true
      }
    }
  }

  def add(value: Array[Byte]): Boolean = add(value, 0)

  /**
   * Remove an item from the queue. If no item is available, an empty byte
   * array is returned.
   *
   * @param transaction true if this should be considered the first part
   *     of a transaction, to be committed or rolled back (put back at the
   *     head of the queue)
   */
  def remove(transaction: Boolean): Option[QItem] = {
    initialized.await
    synchronized {
      if (closed || queueLength == 0) {
        None
      } else {
        val item = _remove(transaction)
        if (transaction) journal.removeTentative() else journal.remove()

        if ((queueLength == 0) && (journal.size >= PersistentQueue.maxJournalSize) &&
            (openTransactions.size == 0)) {
          log.info("Rolling journal file for '%s'", name)
          journal.roll
          journal.saveXid(xidCounter)
        }
        item
      }
    }
  }

  /**
   * Remove an item from the queue. If no item is available, an empty byte
   * array is returned.
   */
  def remove(): Option[QItem] = remove(false)

  def remove(timeoutAbsolute: Long, transaction: Boolean)(f: Option[QItem] => Unit): Unit = {
    synchronized {
      val item = remove(transaction)
      if (item.isDefined) {
        f(item)
      } else if (timeoutAbsolute == 0) {
        f(None)
      } else {
        val w = Waiter(Actor.self)
        waiters += w
        Actor.self.reactWithin((timeoutAbsolute - System.currentTimeMillis) max 0) {
          case ItemArrived => remove(timeoutAbsolute, transaction)(f)
          case TIMEOUT => synchronized {
            waiters -= w
            // race: someone could have done an add() between the timeout and grabbing the lock.
            Actor.self.reactWithin(0) {
              case ItemArrived => f(remove(transaction))
              case TIMEOUT => f(remove(transaction))
            }
          }
        }
      }
    }
  }

  /**
   * Return a transactionally-removed item to the queue. This is a rolled-
   * back transaction.
   */
  def unremove(xid: Int): Unit = {
    initialized.await
    synchronized {
      if (!closed) {
        journal.unremove(xid)
        _unremove(xid)
        if (waiters.size > 0) {
          waiters.remove(0).actor ! ItemArrived
        }
      }
    }
  }

  def confirmRemove(xid: Int): Unit = {
    initialized.await
    synchronized {
      if (!closed) {
        journal.confirmRemove(xid)
        openTransactions.removeKey(xid)
      }
    }
  }

  /**
   * Close the queue's journal file. Not safe to call on an active queue.
   */
  def close = synchronized {
    closed = true
    journal.close()
  }

  def setup(): Unit = synchronized {
    queueSize = 0
    replayJournal
    initialized.countDown
  }

  private def nextXid(): Int = {
    do {
      xidCounter += 1
    } while (openTransactions contains xidCounter)
    xidCounter
  }

  def fillReadBehind(): Unit = {
    journal.fillReadBehind { item =>
      queue += item
      _memoryBytes += item.data.length
    }
  }

  def replayJournal(): Unit = {
    log.info("Replaying transaction journal for '%s'", name)
    xidCounter = 0

    journal.replay(name) {
      case JournalItem.Add(item) =>
        _add(item)
        // when processing the journal, this has to happen after:
        if (!journal.inReadBehind && queueSize >= PersistentQueue.maxMemorySize) {
          log.info("Dropping to read-behind for queue '%s' (%d bytes)", name, queueSize)
          journal.startReadBehind
        }
      case JournalItem.Remove => _remove(false)
      case JournalItem.RemoveTentative => _remove(true)
      case JournalItem.SavedXid(xid) => xidCounter = xid
      case JournalItem.Unremove(xid) => _unremove(xid)
      case JournalItem.ConfirmRemove(xid) => openTransactions.removeKey(xid)
      case x => log.error("Unexpected item in journal: %s", x)
    }
    log.info("Finished transaction journal for '%s' (%d items, %d bytes)", name, queueLength,
             journal.size)
    journal.open
  }


  //  -----  internal implementations

  private def _add(item: QItem): Unit = {
    if (!journal.inReadBehind) {
      queue += item
      _memoryBytes += item.data.length
    }
    _totalItems += 1
    queueSize += item.data.length
    queueLength += 1
  }

  private def _remove(transaction: Boolean): Option[QItem] = {
    if (queue.isEmpty) return None

    val now = System.currentTimeMillis
    val item = queue.dequeue
    val len = item.data.length
    queueSize -= len
    _memoryBytes -= len
    queueLength -= 1
    val xid = if (transaction) nextXid else 0

    // if we're in read-behind mode, scan forward in the journal to keep memory as full as
    // possible. this amortizes the disk overhead across all reads.
    while (journal.inReadBehind && _memoryBytes < PersistentQueue.maxMemorySize) {
      fillReadBehind
      if (!journal.inReadBehind) {
        log.info("Coming out of read-behind for queue '%s'", name)
      }
    }

    val realExpiry = adjustExpiry(item.expiry)
    if ((realExpiry == 0) || (realExpiry >= now)) {
      _currentAge = now - item.addTime
      if (transaction) {
        item.xid = xid
        openTransactions(xid) = item
      }
      Some(item)
    } else {
      _totalExpired += 1
      _remove(transaction)
    }
  }

  private def _unremove(xid: Int) = {
    val item = openTransactions.removeKey(xid).get
    _totalItems += 1
    queueLength += 1
    queueSize += item.data.length
    queue unget item
    _memoryBytes += item.data.length
  }
}


object PersistentQueue {
  @volatile var maxJournalSize: Long = 16 * 1024 * 1024
  @volatile var maxMemorySize: Long = 128 * 1024 * 1024
}
