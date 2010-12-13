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

import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel
import java.util.concurrent.CountDownLatch
import com.twitter.actors.{Actor, TIMEOUT}
import scala.collection.mutable
import com.twitter.{Duration, Time}
import com.twitter.logging.Logger
import config._

class PersistentQueue(val name: String, persistencePath: String, @volatile var config: QueueConfig,
                      queueLookup: Option[(String => Option[PersistentQueue])]) {
  def this(name: String, persistencePath: String, config: QueueConfig) =
    this(name, persistencePath, config, None)

  private case class Waiter(actor: Actor)
  private case object ItemArrived

  private val log = Logger.get(getClass.getName)

  // current size of all data in the queue:
  private var queueSize: Long = 0

  // # of items EVER added to the queue:
  private var _totalItems: Long = 0

  // # of items that were expired by the time they were read:
  private var _totalExpired: Long = 0

  // age (in milliseconds) of the last item read from the queue:
  private var _currentAge: Long = 0

  // # of items thot were discarded because the queue was full:
  private var _totalDiscarded: Long = 0

  // # of items in the queue (including those not in memory)
  private var queueLength: Long = 0

  private var queue = new mutable.Queue[QItem]

  private var _memoryBytes: Long = 0

  private var closed = false
  private var paused = false

  // clients waiting on an item in this queue
  private val waiters = new mutable.ListBuffer[Waiter]

  private var journal =
    new Journal(new File(persistencePath).getCanonicalPath, name, config.syncJournal, config.multifileJournal)

  // track tentative removals
  private var xidCounter: Int = 0
  private val openTransactions = new mutable.HashMap[Int, QItem]
  def openTransactionCount = openTransactions.size
  def openTransactionIds = openTransactions.keys.toSeq.sorted.reverse

  def length: Long = synchronized { queueLength }
  def totalItems: Long = synchronized { _totalItems }
  def bytes: Long = synchronized { queueSize }
  def journalSize: Long = synchronized { journal.size }
  def totalExpired: Long = synchronized { _totalExpired }
  def currentAge: Long = synchronized { if (queueSize == 0) 0 else _currentAge }
  def waiterCount: Long = synchronized { waiters.size }
  def totalDiscarded: Long = synchronized { _totalDiscarded }
  def isClosed: Boolean = synchronized { closed || paused }

  // mostly for unit tests.
  def memoryLength: Long = synchronized { queue.size }
  def memoryBytes: Long = synchronized { _memoryBytes }
  def inReadBehind = synchronized { journal.inReadBehind }

  if (!config.keepJournal) journal.erase()

  val expireQueue = config.expireToQueue.flatMap { name => queueLookup.flatMap(_(name)) }

  // FIXME
  def dumpConfig(): Array[String] = synchronized {
    Array(
      "max_items=" + config.maxItems,
      "max_size=" + config.maxSize,
      "max_age=" + config.maxAge,
      "max_journal_size=" + config.maxJournalSize,
      "max_memory_size=" + config.maxMemorySize,
      "max_journal_overflow=" + config.maxJournalOverflow,
      "discard_old_when_full=" + config.discardOldWhenFull,
      "journal=" + config.keepJournal,
      "sync_journal=" + config.syncJournal,
      "move_expired_to=" + config.expireToQueue.getOrElse("(none)")
    )
  }

  def dumpStats(): Array[(String, String)] = synchronized {
    Array(
      ("items", length.toString),
      ("bytes", bytes.toString),
      ("total_items", totalItems.toString),
      ("logsize", journalSize.toString),
      ("expired_items", totalExpired.toString),
      ("mem_items", memoryLength.toString),
      ("mem_bytes", memoryBytes.toString),
      ("age", currentAge.toString),
      ("discarded", totalDiscarded.toString),
      ("waiters", waiterCount.toString),
      ("open_transactions", openTransactionCount.toString)
    )
  }

  private final def adjustExpiry(startingTime: Long, expiry: Long): Long = {
    config.maxAge.map { m =>
      val maxExpiry = startingTime + m.inMilliseconds
      if (expiry > 0) (expiry min maxExpiry) else maxExpiry
    }.getOrElse(expiry)
  }

  final def rollJournal() {
    if (config.keepJournal && !config.multifileJournal) {
      synchronized {
        log.info("Rolling journal file for '%s' (qsize=%d)", name, queueSize)
        journal.roll(xidCounter, openTransactionIds map { openTransactions(_) }, queue)
      }
    }
  }

  final def checkRotateJournal() {
    if (config.keepJournal && config.multifileJournal && journal.size > config.maxJournalSize) {
      synchronized {
        log.info("Rotating journal file for '%s'", name)
        journal.rotate()
      }
    }
  }

  /**
   * Add a value to the end of the queue, transactionally.
   */
  def add(value: Array[Byte], expiry: Long): Boolean = synchronized {
    if (closed || value.size > config.maxItemSize) return false
    while (queueLength >= config.maxItems || queueSize >= config.maxSize) {
      if (!config.discardOldWhenFull) return false
      _remove(false)
      _totalDiscarded += 1
      if (config.keepJournal) journal.remove()
    }

    val now = Time.now.inMilliseconds
    val item = QItem(now, adjustExpiry(now, expiry), value, 0)
    if (config.keepJournal && !journal.inReadBehind) {
      if (journal.size > config.maxJournalSize * config.maxJournalOverflow && queueSize < config.maxJournalSize) {
        // force re-creation of the journal.
        rollJournal()
      }
      if (queueSize >= config.maxMemorySize) {
        log.info("Dropping to read-behind for queue '%s' (%d bytes)", name, queueSize)
        journal.startReadBehind
      }
    }
    checkRotateJournal()
    _add(item)
    if (config.keepJournal) journal.add(item)
    if (waiters.size > 0) {
      waiters.remove(0).actor ! ItemArrived
    }
    true
  }

  def add(value: Array[Byte]): Boolean = add(value, 0)

  /**
   * Peek at the head item in the queue, if there is one.
   */
  def peek(): Option[QItem] = {
    synchronized {
      if (closed || paused || queueLength == 0) {
        None
      } else {
        _peek()
      }
    }
  }

  /**
   * Remove and return an item from the queue, if there is one.
   *
   * @param transaction true if this should be considered the first part
   *     of a transaction, to be committed or rolled back (put back at the
   *     head of the queue)
   */
  def remove(transaction: Boolean): Option[QItem] = {
    synchronized {
      if (closed || paused || queueLength == 0) {
        None
      } else {
        val item = _remove(transaction)
        if (config.keepJournal) {
          if (transaction) journal.removeTentative() else journal.remove()

          if ((queueLength == 0) && (journal.size >= config.maxJournalSize)) {
            rollJournal()
          }
          checkRotateJournal()
        }
        item
      }
    }
  }

  /**
   * Remove and return an item from the queue, if there is one.
   */
  def remove(): Option[QItem] = remove(false)

  def operateReact(op: => Option[QItem], timeoutAbsolute: Long)(f: Option[QItem] => Unit): Unit = {
    operateOrWait(op, timeoutAbsolute) match {
      case (item, None) =>
        f(item)
      case (None, Some(w)) =>
        Actor.self.reactWithin((timeoutAbsolute - Time.now.inMilliseconds) max 0) {
          case ItemArrived => operateReact(op, timeoutAbsolute)(f)
          case TIMEOUT => synchronized {
            waiters -= w
            // race: someone could have done an add() between the timeout and grabbing the lock.
            Actor.self.reactWithin(0) {
              case ItemArrived => f(op)
              case TIMEOUT => f(op)
            }
          }
        }
      case _ => throw new RuntimeException()
    }
  }

  def operateReceive(op: => Option[QItem], timeoutAbsolute: Long): Option[QItem] = {
    operateOrWait(op, timeoutAbsolute) match {
      case (item, None) =>
        item
      case (None, Some(w)) =>
        val gotSomething = Actor.self.receiveWithin((timeoutAbsolute - Time.now.inMilliseconds) max 0) {
          case ItemArrived => true
          case TIMEOUT => false
        }
        if (gotSomething) {
          operateReceive(op, timeoutAbsolute)
        } else {
          synchronized { waiters -= w }
          // race: someone could have done an add() between the timeout and grabbing the lock.
          Actor.self.receiveWithin(0) {
            case ItemArrived =>
            case TIMEOUT =>
          }
          op
        }
      case _ => throw new RuntimeException()
    }
  }

  def removeReact(timeoutAbsolute: Long, transaction: Boolean)(f: Option[QItem] => Unit): Unit = {
    operateReact(remove(transaction), timeoutAbsolute)(f)
  }

  def removeReceive(timeoutAbsolute: Long, transaction: Boolean): Option[QItem] = {
    operateReceive(remove(transaction), timeoutAbsolute)
  }

  def peekReact(timeoutAbsolute: Long)(f: Option[QItem] => Unit): Unit = {
    operateReact(peek, timeoutAbsolute)(f)
  }

  def peekReceive(timeoutAbsolute: Long): Option[QItem] = {
    operateReceive(peek, timeoutAbsolute)
  }

  /**
   * Perform an operation on the next item from the queue, if there is one.
   * If the queue is closed, returns immediately. Otherwise, if a timeout is passed in, the
   * current actor is added to the wait-list, and will receive `ItemArrived` when an item is
   * available (or the queue is closed).
   */
  private def operateOrWait(op: => Option[QItem], timeoutAbsolute: Long): (Option[QItem], Option[Waiter]) = synchronized {
    val item = op
    if (!item.isDefined && !closed && !paused && timeoutAbsolute > 0) {
      val w = Waiter(Actor.self)
      waiters += w
      (None, Some(w))
    } else {
      (item, None)
    }
  }

  /**
   * Return a transactionally-removed item to the queue. This is a rolled-
   * back transaction.
   */
  def unremove(xid: Int): Unit = {
    synchronized {
      if (!closed) {
        if (config.keepJournal) journal.unremove(xid)
        _unremove(xid)
        if (waiters.size > 0) {
          waiters.remove(0).actor ! ItemArrived
        }
      }
    }
  }

  def confirmRemove(xid: Int): Unit = {
    synchronized {
      if (!closed) {
        if (config.keepJournal) journal.confirmRemove(xid)
        openTransactions.remove(xid)
      }
    }
  }

  def flush(): Unit = {
    while (remove(false).isDefined) { }
  }

  /**
   * Close the queue's journal file. Not safe to call on an active queue.
   */
  def close(): Unit = synchronized {
    closed = true
    if (config.keepJournal) journal.close()
    for (w <- waiters) {
      w.actor ! ItemArrived
    }
    waiters.clear()
  }

  def pauseReads(): Unit = synchronized {
    paused = true
    for (w <- waiters) {
      w.actor ! ItemArrived
    }
    waiters.clear()
  }

  def resumeReads(): Unit = synchronized {
    paused = false
  }

  def setup(): Unit = synchronized {
    queueSize = 0
    replayJournal
  }

  def destroyJournal(): Unit = synchronized {
    if (config.keepJournal) journal.erase()
  }

  private final def nextXid(): Int = {
    do {
      xidCounter += 1
    } while (openTransactions contains xidCounter)
    xidCounter
  }

  private final def fillReadBehind(): Unit = {
    // if we're in read-behind mode, scan forward in the journal to keep memory as full as
    // possible. this amortizes the disk overhead across all reads.
    while (config.keepJournal && journal.inReadBehind && _memoryBytes < config.maxMemorySize) {
      journal.fillReadBehind { item =>
        queue += item
        _memoryBytes += item.data.length
      }
      if (!journal.inReadBehind) {
        log.info("Coming out of read-behind for queue '%s'", name)
      }
    }
  }

  def replayJournal(): Unit = {
    if (!config.keepJournal) return

    log.info("Replaying transaction journal for '%s'", name)
    xidCounter = 0

    journal.replay(name) {
      case JournalItem.Add(item) =>
        _add(item)
        // when processing the journal, this has to happen after:
        if (!journal.inReadBehind && queueSize >= config.maxMemorySize) {
          log.info("Dropping to read-behind for queue '%s' (%d bytes)", name, queueSize)
          journal.startReadBehind
        }
      case JournalItem.Remove => _remove(false)
      case JournalItem.RemoveTentative => _remove(true)
      case JournalItem.SavedXid(xid) => xidCounter = xid
      case JournalItem.Unremove(xid) => _unremove(xid)
      case JournalItem.ConfirmRemove(xid) => openTransactions.remove(xid)
      case x => log.error("Unexpected item in journal: %s", x)
    }

    log.info("Finished transaction journal for '%s' (%d items, %d bytes)", name, queueLength,
             journal.size)
    journal.open

    // now, any unfinished transactions must be backed out.
    for (xid <- openTransactionIds) {
      journal.unremove(xid)
      _unremove(xid)
    }
  }


  //  -----  internal implementations

  private def _add(item: QItem): Unit = {
    discardExpired(config.maxExpireSweep)
    if (!journal.inReadBehind) {
      queue += item
      _memoryBytes += item.data.length
    }
    _totalItems += 1
    queueSize += item.data.length
    queueLength += 1
  }

  private def _peek(): Option[QItem] = {
    discardExpired(config.maxExpireSweep)
    if (queue.isEmpty) None else Some(queue.front)
  }

  private def _remove(transaction: Boolean): Option[QItem] = {
    discardExpired(config.maxExpireSweep)
    if (queue.isEmpty) return None

    val now = Time.now.inMilliseconds
    val item = queue.dequeue
    val len = item.data.length
    queueSize -= len
    _memoryBytes -= len
    queueLength -= 1
    val xid = if (transaction) nextXid else 0

    fillReadBehind
    _currentAge = now - item.addTime
    if (transaction) {
      item.xid = xid
      openTransactions(xid) = item
    }
    Some(item)
  }

  final def discardExpired(max: Int): Int = {
    if (queue.isEmpty || journal.isReplaying || max <= 0) {
      0
    } else {
      val realExpiry = adjustExpiry(queue.front.addTime, queue.front.expiry)
      if ((realExpiry != 0) && (realExpiry <= Time.now.inMilliseconds)) {
        _totalExpired += 1
        val item = queue.dequeue
        val len = item.data.length
        queueSize -= len
        _memoryBytes -= len
        queueLength -= 1
        fillReadBehind
        if (config.keepJournal) journal.remove()
        expireQueue.foreach { _.add(item.data, 0) }
        1 + discardExpired(max - 1)
      } else {
        0
      }
    }
  }

  private def _unremove(xid: Int) = {
    openTransactions.remove(xid) map { item =>
      queueLength += 1
      queueSize += item.data.length
      item +=: queue
      _memoryBytes += item.data.length
    }
  }
}
