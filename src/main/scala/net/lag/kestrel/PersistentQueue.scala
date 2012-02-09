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
import java.util.concurrent.{CountDownLatch, Executor, ScheduledExecutorService}
import scala.collection.mutable
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.ostrich.stats.Stats
import com.twitter.util._
import config._

class PersistentQueue(val name: String, persistencePath: String, @volatile var config: QueueConfig,
                      timer: Timer, journalSyncScheduler: ScheduledExecutorService,
                      queueLookup: Option[(String => Option[PersistentQueue])]) {
  def this(name: String, persistencePath: String, config: QueueConfig, timer: Timer, journalSyncScheduler: ScheduledExecutorService) =
    this(name, persistencePath, config, timer, journalSyncScheduler, None)

  private val log = Logger.get(getClass.getName)

  private val isFanout = (name contains '+')

  // current size of all data in the queue:
  private var queueSize: Long = 0

  // age of the last item read from the queue:
  private var _currentAge: Duration = 0.milliseconds
  
  // time the queue was created
  private var _createTime = Time.now

  def statNamed(statName: String) = "q/" + name + "/" + statName

  // # of items EVER added to the queue:
  val totalItems = Stats.getCounter(statNamed("total_items"))
  totalItems.reset()

  // # of items that were expired by the time they were read:
  val totalExpired = Stats.getCounter(statNamed("expired_items"))
  totalExpired.reset()

  // # of items thot were discarded because the queue was full:
  val totalDiscarded = Stats.getCounter(statNamed("discarded"))
  totalDiscarded.reset()

  // # of times this queue has been flushed:
  val totalFlushes = Stats.getCounter(statNamed("total_flushes"))
  totalFlushes.reset()

  // # of items in the queue (including those not in memory)
  private var queueLength: Long = 0

  private var queue = new mutable.Queue[QItem]

  private var _memoryBytes: Long = 0

  private var closed = false
  private var paused = false

  private var journal =
    new Journal(new File(persistencePath).getCanonicalFile, name, journalSyncScheduler, config.syncJournal)

  private val waiters = new DeadlineWaitQueue(timer)

  // track tentative removals
  private var xidCounter: Int = 0
  private val openTransactions = new mutable.HashMap[Int, QItem]
  private def openTransactionIds = openTransactions.keys.toSeq.sorted.reverse
  def openTransactionCount = synchronized { openTransactions.size }

  def length: Long = synchronized { queueLength }
  def bytes: Long = synchronized { queueSize }
  def maxMemoryBytes: Long = synchronized { config.maxMemorySize.inBytes }
  def journalSize: Long = synchronized { journal.size }
  def journalTotalSize: Long = journal.archivedSize + journalSize
  def currentAge: Duration = synchronized { if (queueSize == 0) 0.milliseconds else _currentAge }
  def waiterCount: Long = synchronized { waiters.size }
  def isClosed: Boolean = synchronized { closed || paused }

  // mostly for unit tests.
  def memoryLength: Long = synchronized { queue.size }
  def memoryBytes: Long = synchronized { _memoryBytes }
  def inReadBehind = synchronized { journal.inReadBehind }

  if (!config.keepJournal) journal.erase()

  @volatile var expireQueue: Option[PersistentQueue] = config.expireToQueue.flatMap { name => queueLookup.flatMap(_(name)) }

  def dumpStats(): Array[(String, String)] = synchronized {
    Array(
      ("items", length.toString),
      ("bytes", bytes.toString),
      ("total_items", totalItems().toString),
      ("logsize", journalSize.toString),
      ("expired_items", totalExpired().toString),
      ("mem_items", memoryLength.toString),
      ("mem_bytes", memoryBytes.toString),
      ("age", currentAge.inMilliseconds.toString),
      ("discarded", totalDiscarded().toString),
      ("waiters", waiterCount.toString),
      ("open_transactions", openTransactionCount.toString),
      ("total_flushes", totalFlushes().toString)
    )
  }

  def gauge(gaugeName: String, value: => Double) = Stats.addGauge("q/" + name + "/" + gaugeName)(value)

  gauge("items", length)
  gauge("bytes", bytes)
  gauge("journal_size", journalTotalSize)
  gauge("mem_items", memoryLength)
  gauge("mem_bytes", memoryBytes)
  gauge("age_msec", currentAge.inMilliseconds)
  gauge("waiters", waiterCount)
  gauge("open_transactions", openTransactionCount)

  private final def adjustExpiry(startingTime: Time, expiry: Option[Time]): Option[Time] = {
    if (config.maxAge.isDefined) {
      val maxExpiry = startingTime + config.maxAge.get
      if (expiry.isDefined) Some(expiry.get min maxExpiry) else Some(maxExpiry)
    } else {
      expiry
    }
  }

  /**
   * Check if this Queue is eligible for expiration by way of it being empty
   * and it's age being greater than or equal to maxQueueAge
   */
  def isReadyForExpiration: Boolean = {
    // Don't even bother if the maxQueueAge is None
    if(config.maxQueueAge.isDefined && config.maxQueueAge.get == None) {
      false
    } else {
      if(queue.isEmpty && config.maxQueueAge.isDefined && Time.now > _createTime + config.maxQueueAge.get) {
        true
      } else {
        false
      }
    }
  }

  // you are holding the lock, and config.keepJournal is true.
  private def checkRotateJournal() {
    /*
     * if the queue is empty, and the journal is larger than defaultJournalSize, rebuild it.
     * if the queue is smaller than maxMemorySize, and the combined journals are larger than
     *   maxJournalSize, rebuild them. (we are not in read-behind.)
     * if the current journal is larger than maxMemorySize, rotate to a new file. if the combined
     *   journals are larger than maxJournalSize, checkpoint in preparation for rebuilding the
     *   older files in the background.
     */
    if ((journal.size >= config.defaultJournalSize.inBytes && queueLength == 0) ||
        (journal.size + journal.archivedSize > config.maxJournalSize.inBytes &&
         queueSize < config.maxMemorySize.inBytes)) {
      log.info("Rewriting journal file for '%s' (qsize=%d)", name, queueSize)
      journal.rewrite(openTransactionIds.map { openTransactions(_) }, queue)
    } else if (journal.size > config.maxMemorySize.inBytes) {
      log.info("Rotating journal file for '%s' (qsize=%d)", name, queueSize)
      val setCheckpoint = (journal.size + journal.archivedSize > config.maxJournalSize.inBytes)
      journal.rotate(openTransactionIds.map { openTransactions(_) }, setCheckpoint)
    }
  }

  // for tests.
  def forceRewrite() {
    synchronized {
      if (config.keepJournal) {
        log.info("Rewriting journal file for '%s' (qsize=%d)", name, queueSize)
        journal.rewrite(openTransactionIds.map { openTransactions(_) }, queue)
      }
    }
  }

  /**
   * Add a value to the end of the queue, transactionally.
   */
  def add(value: Array[Byte], expiry: Option[Time], xid: Option[Int], addTime: Time): Boolean = {
    val future = synchronized {
      if (closed || value.size > config.maxItemSize.inBytes) return false
      if (config.fanoutOnly && !isFanout) return true
      while (queueLength >= config.maxItems || queueSize >= config.maxSize.inBytes) {
        if (!config.discardOldWhenFull) return false
        _remove(false, None)
        totalDiscarded.incr()
        if (config.keepJournal) journal.remove()
      }

      val item = QItem(addTime, adjustExpiry(Time.now, expiry), value, 0)
      if (config.keepJournal) {
        checkRotateJournal()
        if (!journal.inReadBehind && (queueSize >= config.maxMemorySize.inBytes)) {
          log.info("Dropping to read-behind for queue '%s' (%s)", name, queueSize.bytes.toHuman())
          journal.startReadBehind()
        }
      }
      if (xid != None) openTransactions.remove(xid.get)
      _add(item)
      if (config.keepJournal) {
        xid match {
          case None => journal.add(item)
          case Some(xid) => journal.continue(xid, item)
        }
      } else {
        Future.void()
      }
    }
    waiters.trigger()
    // for now, don't wait:
    //future()
    true
  }

  def add(value: Array[Byte]): Boolean = add(value, None, None, Time.now)
  def add(value: Array[Byte], expiry: Option[Time]): Boolean = add(value, expiry, None, Time.now)

  def continue(xid: Int, value: Array[Byte]): Boolean = add(value, None, Some(xid), Time.now)
  def continue(xid: Int, value: Array[Byte], expiry: Option[Time]): Boolean = add(value, expiry, Some(xid), Time.now)

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
    val removedItem = synchronized {
      if (closed || paused || queueLength == 0) {
        None
      } else {
        val item = _remove(transaction, None)
        if (config.keepJournal && item.isDefined) {
          if (transaction) journal.removeTentative(item.get.xid) else journal.remove()
          checkRotateJournal()
        }

        item
      }
    }

    removedItem.foreach { qItem =>
      val usec = (Time.now - qItem.addTime).inMilliseconds.toInt max 0
      Stats.addMetric("delivery_latency_msec", usec)
      Stats.addMetric("q/" + name + "/delivery_latency_msec", usec)
    }
    removedItem
  }

  /**
   * Remove and return an item from the queue, if there is one.
   */
  def remove(): Option[QItem] = remove(false)

  private def waitOperation(op: => Option[QItem], startTime: Time, deadline: Option[Time],
                            future: Promise[Option[QItem]]) {
    val item = op
    if (synchronized {
      if (!item.isDefined && !closed && !paused && deadline.isDefined && deadline.get > Time.now) {
        // if we get woken up, try again with the same deadline.
        def onTrigger() = waitOperation(op, startTime, deadline, future)
        def onTimeout() {
          val msec = (Time.now - startTime).inMilliseconds.toInt
          Stats.addMetric("get_timeout_msec", msec)
          Stats.addMetric("q/" + name + "/get_timeout_msec", msec)
          future.setValue(None)
        }
        val w = waiters.add(deadline.get, onTrigger, onTimeout)
        // FIXME: use onCancellation when util-core is bumped.
        future.linkTo(new CancellableSink({ waiters.remove(w) }))
        false
      } else {
        true
      }
    }) future.setValue(item)
  }

  final def waitRemove(deadline: Option[Time], transaction: Boolean): Future[Option[QItem]] = {
    val startTime = Time.now
    val promise = new Promise[Option[QItem]]()
    waitOperation(remove(transaction), startTime, deadline, promise)
    // if an item was handed off immediately, track latency of the "get" operation
    if (promise.isDefined) {
      val statName = if (promise().isDefined) "get_hit_latency_usec" else "get_miss_latency_usec"
      val usec = (Time.now - startTime).inMicroseconds.toInt max 0
      Stats.addMetric(statName, usec)
      Stats.addMetric("q/" + name + "/" + statName, usec)
    }
    promise
  }

  final def waitPeek(deadline: Option[Time]): Future[Option[QItem]] = {
    val promise = new Promise[Option[QItem]]()
    waitOperation(peek(), Time.now, deadline, promise)
    promise
  }

  /**
   * Return a transactionally-removed item to the queue. This is a rolled-
   * back transaction.
   */
  def unremove(xid: Int) {
    synchronized {
      if (!closed) {
        if (config.keepJournal) journal.unremove(xid)
        _unremove(xid)
        waiters.trigger()
      }
    }
  }

  def confirmRemove(xid: Int) {
    synchronized {
      if (!closed) {
        if (config.keepJournal) journal.confirmRemove(xid)
        openTransactions.remove(xid)
      }
    }
  }

  def flush() {
    while (remove(false).isDefined) { }
    totalFlushes.incr()
  }

  /**
   * Close the queue's journal file. Not safe to call on an active queue.
   */
  def close() {
    synchronized {
      closed = true
      if (config.keepJournal) journal.close()
      waiters.triggerAll()
    }
  }

  def pauseReads() {
    synchronized {
      paused = true
      waiters.triggerAll()
    }
  }

  def resumeReads() {
    synchronized {
      paused = false
    }
  }

  def setup() {
    synchronized {
      queueSize = 0
      replayJournal()
    }
  }

  def destroyJournal() {
    synchronized {
      if (config.keepJournal) journal.erase()
    }
  }

  // Remove various stats related to the queue
  def removeStats() {
    Stats.removeCounter(statNamed("total_items"))
    Stats.removeCounter(statNamed("expired_items"))
    Stats.removeCounter(statNamed("discarded"))
    Stats.removeCounter(statNamed("total_flushes"))
    Stats.clearGauge(statNamed("items"))
    Stats.clearGauge(statNamed("bytes"))
    Stats.clearGauge(statNamed("journal_size"))
    Stats.clearGauge(statNamed("mem_items"))
    Stats.clearGauge(statNamed("mem_bytes"))
    Stats.clearGauge(statNamed("age_msec"))
    Stats.clearGauge(statNamed("waiters"))
    Stats.clearGauge(statNamed("open_transactions"))
  }

  private final def nextXid(): Int = {
    do {
      xidCounter += 1
    } while ((openTransactions contains xidCounter) || (xidCounter == 0))
    xidCounter
  }

  private final def fillReadBehind() {
    // if we're in read-behind mode, scan forward in the journal to keep memory as full as
    // possible. this amortizes the disk overhead across all reads.
    while (config.keepJournal && journal.inReadBehind && _memoryBytes < config.maxMemorySize.inBytes) {
      journal.fillReadBehind { item =>
        queue += item
        _memoryBytes += item.data.length
      } { checkpoint =>
        log.info("Rewriting journal file from checkpoint for '%s' (qsize=%d)", name, queueSize)
        journal.startPack(checkpoint, openTransactions.values.toList, queue.toList)
      }
      if (!journal.inReadBehind) {
        log.info("Coming out of read-behind for queue '%s'", name)
      }
    }
  }

  def replayJournal() {
    if (!config.keepJournal) return

    log.info("Replaying transaction journal for '%s'", name)
    xidCounter = 0

    journal.replay {
      case JournalItem.Add(item) =>
        _add(item)
        // when processing the journal, this has to happen after:
        if (!journal.inReadBehind && queueSize >= config.maxMemorySize.inBytes) {
          log.info("Dropping to read-behind for queue '%s' (%d bytes)", name, queueSize)
          journal.startReadBehind()
        }
      case JournalItem.Remove => _remove(false, None)
      case JournalItem.RemoveTentative(xid) =>
        _remove(true, Some(xid))
        xidCounter = xid
      case JournalItem.SavedXid(xid) => xidCounter = xid
      case JournalItem.Unremove(xid) => _unremove(xid)
      case JournalItem.ConfirmRemove(xid) => openTransactions.remove(xid)
      case JournalItem.Continue(item, xid) =>
        openTransactions.remove(xid)
        _add(item)
      case x => log.error("Unexpected item in journal: %s", x)
    }

    log.info("Finished transaction journal for '%s' (%d items, %d bytes) xid=%d", name, queueLength,
             journal.size, xidCounter)
    journal.open()

    // now, any unfinished transactions must be backed out.
    for (xid <- openTransactionIds) {
      journal.unremove(xid)
      _unremove(xid)
    }
  }


  //  -----  internal implementations

  private def _add(item: QItem) {
    discardExpired(config.maxExpireSweep)
    if (!journal.inReadBehind) {
      queue += item
      _memoryBytes += item.data.length
    }
    totalItems.incr()
    queueSize += item.data.length
    queueLength += 1
  }

  private def _peek(): Option[QItem] = {
    discardExpired(config.maxExpireSweep)
    if (queue.isEmpty) None else Some(queue.front)
  }

  private def _remove(transaction: Boolean, xid: Option[Int]): Option[QItem] = {
    discardExpired(config.maxExpireSweep)
    if (queue.isEmpty) return None

    val now = Time.now
    val item = queue.dequeue()
    val len = item.data.length
    queueSize -= len
    _memoryBytes -= len
    queueLength -= 1
    fillReadBehind()
    _currentAge = now - item.addTime
    if (transaction) {
      item.xid = xid.getOrElse { nextXid() }
      openTransactions(item.xid) = item
    }
    Some(item)
  }

  final def discardExpired(max: Int): Int = {
    val itemsToRemove = synchronized {
      var continue = true
      val toRemove = new mutable.ListBuffer[QItem]
      while (continue) {
        if (queue.isEmpty || journal.isReplaying) {
          continue = false
        } else {
          val realExpiry = adjustExpiry(queue.front.addTime, queue.front.expiry)
          if (realExpiry.isDefined && realExpiry.get < Time.now) {
            totalExpired.incr()
            val item = queue.dequeue()
            val len = item.data.length
            queueSize -= len
            _memoryBytes -= len
            queueLength -= 1
            fillReadBehind()
            if (config.keepJournal) journal.remove()
            toRemove += item
          } else {
            continue = false
          }
        }
      }
      toRemove
    }

    expireQueue.foreach { q =>
      itemsToRemove.foreach { item => q.add(item.data, None) }
    }
    itemsToRemove.size
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
