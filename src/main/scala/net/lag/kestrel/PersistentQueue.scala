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

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle.thrift.ClientId
import com.twitter.logging.Logger
import com.twitter.ostrich.stats.{Metric, Stats}
import com.twitter.util._
import config._
import java.io._
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable
import scala.collection.JavaConversions._

class PersistentQueue(val name: String, persistencePath: PersistentStreamContainer, @volatile var config: QueueConfig,
                      timer: Timer, queueLookup: Option[(String => Option[PersistentQueue])]) {

  def this(name: String, persistencePath: PersistentStreamContainer, config: QueueConfig, timer: Timer) =
    this(name, persistencePath, config, timer, None)

  // TODO: These constructors are retained to avoid a large scale change to existing tests.
  // Hope to remove this after completing refactoring of the
  // affected tests
  def this(name: String, persistencePath: String, config: QueueConfig,
           timer: Timer, journalSyncScheduler: ScheduledExecutorService,
           queueLookup: Option[(String => Option[PersistentQueue])]) =
    this(name, new LocalDirectory(persistencePath, journalSyncScheduler), config, timer, queueLookup)

  def this(name: String, persistencePath: String, config: QueueConfig, timer: Timer, journalSyncScheduler: ScheduledExecutorService) =
    this(name, new LocalDirectory(persistencePath, journalSyncScheduler), config, timer, None)

  private val log = Logger.get(getClass.getName)

  private val isFanout = (name contains '+')

  // current size of all data in the queue:
  private var queueSize: Long = 0

  // timestamp of the last item read from the queue:
  private var _currentAge: Time = Time.epoch

  // time the queue was created
  private var _createTime = Time.now

  def statNamed(statName: String) = "q/" + name + "/" + statName

  // # of items EVER added to the queue:
  val putItems = new AtomicLong(0)
  Stats.removeCounter(statNamed("total_items"))
  Stats.makeCounter(statNamed("total_items"), putItems)
  Stats.removeCounter(statNamed("put_items"))
  Stats.makeCounter(statNamed("put_items"), putItems)

  // # of bytes EVER added to the queue:
  val putBytes = new AtomicLong(0)
  Stats.removeCounter(statNamed("put_bytes"))
  Stats.makeCounter(statNamed("put_bytes"), putBytes)

  // # of items EVER received as hit or miss:
  val getItemsHit = new AtomicLong(0)
  Stats.removeCounter(statNamed("get_items_hit"))
  Stats.makeCounter(statNamed("get_items_hit"), getItemsHit)
  val getItemsMiss = new AtomicLong(0)
  Stats.removeCounter(statNamed("get_items_miss"))
  Stats.makeCounter(statNamed("get_items_miss"), getItemsMiss)

  // # of transactions attempted/canceled
  val totalTransactions = Stats.getCounter(statNamed("transactions"))
  totalTransactions.reset()
  val totalCanceledTransactions = Stats.getCounter(statNamed("canceled_transactions"))
  totalCanceledTransactions.reset()

  // # of items that were expired by the time they were read:
  val totalExpired = Stats.getCounter(statNamed("expired_items"))
  totalExpired.reset()

  // # of items thot were discarded because the queue was full:
  val totalDiscarded = Stats.getCounter(statNamed("discarded"))
  totalDiscarded.reset()

  // # of times this queue has been flushed:
  val totalFlushes = Stats.getCounter(statNamed("total_flushes"))
  totalFlushes.reset()

  val totalRewrites = Stats.getCounter(statNamed("journal_rewrites"))
  totalRewrites.reset()
  val rewriteMetric = Stats.getMetric(statNamed("journal_rewrite_usec"))
  rewriteMetric.clear()
  val totalRotates = Stats.getCounter(statNamed("journal_rotations"))
  totalRotates.reset()
  val rotationMetric = Stats.getMetric(statNamed("journal_rotation_usec"))
  rotationMetric.clear()

  private var allowRewrites = true

  // # of items in the queue (including those not in memory)
  private var queueLength: Long = 0

  // We use java LinkedList here for the same reason we use java LinkedHashMap
  // below: the scala datastructure (Queue) is susceptible to gc nepotism.
  private var queue: java.util.Deque[QItem] = new java.util.LinkedList[QItem]

  private var _memoryBytes: Long = 0

  private var closed = false
  private var paused = false

  private var journal =
    new Journal(persistencePath, name, config.syncJournal)

  private val waiters = new DeadlineWaitQueue(timer)

  // track tentative removals
  private var xidCounter: Int = 0
  private val openTransactions = new mutable.HashMap[Int, QItem]

  // We use java LinkedHashMap rather than the scala impl because of VM-406.
  // A bug in LinkedHashMap causes linked entries to pile up in old gen under load.
  // The bug has been fixed in the twitter jdk but not yet in scala. 
  private val transactionExpiryList = new java.util.LinkedHashMap[Int, Time]

  private def openTransactionIds = openTransactions.keys.toSeq.sorted.reverse
  def openTransactionCount = synchronized { openTransactions.size }

  def length: Long = synchronized { queueLength }
  def bytes: Long = synchronized { queueSize }
  def maxItems: Int = synchronized { config.maxItems }
  def maxBytes: Long = synchronized { config.maxSize.inBytes }
  def maxMemoryBytes: Long = synchronized { config.maxMemorySize.inBytes }
  def journalSize: Long = synchronized { journal.size }
  def journalTotalSize: Long = journal.archivedSize + journalSize
  def currentAge: Duration = synchronized {
    if (queueSize == 0) 0.milliseconds else Time.now - _currentAge
  }
  def waiterCount: Long = synchronized { waiters.size }
  def isClosed: Boolean = synchronized { closed || paused }
  def createTime: Long = synchronized { _createTime.inSeconds }

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
      ("total_items", putItems.toString),
      ("logsize", journalSize.toString),
      ("expired_items", totalExpired().toString),
      ("mem_items", memoryLength.toString),
      ("mem_bytes", memoryBytes.toString),
      ("age", currentAge.inMilliseconds.toString),
      ("discarded", totalDiscarded().toString),
      ("waiters", waiterCount.toString),
      ("open_transactions", openTransactionCount.toString),
      ("transactions", totalTransactions().toString),
      ("canceled_transactions", totalCanceledTransactions().toString),
      ("total_flushes", totalFlushes().toString),
      ("journal_rewrites", totalRewrites().toString),
      ("journal_rotations", totalRotates().toString)
    )
  }

  def gauge(gaugeName: String, value: => Double) = Stats.addGauge("q/" + name + "/" + gaugeName)(value)

  gauge("items", length)
  gauge("max_items", maxItems)
  gauge("bytes", bytes)
  gauge("max_bytes", maxBytes)
  gauge("journal_size", journalTotalSize)
  gauge("mem_items", memoryLength)
  gauge("mem_bytes", memoryBytes)
  gauge("age_msec", currentAge.inMilliseconds)
  gauge("waiters", waiterCount)
  gauge("open_transactions", openTransactionCount)
  gauge("create_time", createTime)

  def metric(metricName: String): Metric = {
    val metric = Stats.getMetric(metricName)
    metric.clear()
    metric
  }

  // see KestrelHandler
  metric(statNamed("set_latency_usec")) 
  metric(statNamed("get_timeout_msec"))
  metric(statNamed("delivery_latency_msec"))
  metric(statNamed("get_hit_latency_usec"))
  metric(statNamed("get_miss_latency_usec"))

  // readbehind
  val fillReadBehindMetric = metric(statNamed("fill_readbehind_usec"))
  
  // internal datastructure metrics
  val txMapSizeMetric = metric(statNamed("tx_map_size"))
  val txExpireListSizeMetric = metric(statNamed("tx_expire_list_size"))
  val queueSizeMetric = metric(statNamed("queue_size"))

  // request size
  val requestSizeMetric = metric(statNamed("request_size"))
  
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
   * and its age being greater than or equal to maxQueueAge
   */
  def isReadyForExpiration: Boolean = {
    // Don't even bother if the maxQueueAge is None
    if (config.maxQueueAge.isDefined && queue.isEmpty && Time.now > _createTime + config.maxQueueAge.get) {
      true
    } else {
      false
    }
  }

  private def disallowRewritesForDelay() {
    config.minJournalCompactDelay.foreach { delay =>
      allowRewrites = false
      timer.schedule(delay.fromNow) {
        PersistentQueue.this.synchronized { allowRewrites = true }
      }
    }
  }

  /**
   * Check if this Queue has been enabled for client tracing
   */
  def shouldTraceQOps: Boolean = {
    config.enableTrace
  }

  def persistChanges() {
    journal.persistChanges()
  }

  private[this] def rewriteJournal() {
    val elapsed = Stopwatch.start()
    journal.rewrite(openTransactionIds.map { openTransactions(_) }, queue)
    rewriteMetric.add(elapsed().inMicroseconds.toInt)
    totalRewrites.incr()
  }

  private[this] def rotateJournal(setCheckpoint: Boolean) {
    val elapsed = Stopwatch.start()
    journal.rotate(openTransactionIds.map { openTransactions(_) }, setCheckpoint)
    rotationMetric.add(elapsed().inMicroseconds.toInt)
    totalRotates.incr()
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
    if (!config.disableAggressiveRewrites) {
      if (journal.size >= config.defaultJournalSize.inBytes && queueLength == 0) {
        log.info("Rewriting journal file for '%s' (qsize=0)", name)
        rewriteJournal()
      } else if (allowRewrites &&
        journal.size + journal.archivedSize > config.maxJournalSize.inBytes &&
        queueSize < config.maxMemorySize.inBytes) {
        log.info("Rewriting journal file for '%s' (qsize=%d)", name, queueSize)
        rewriteJournal()
        config.minJournalCompactDelay.foreach { delay =>
          allowRewrites = false
          timer.schedule(delay.fromNow) {
            PersistentQueue.this.synchronized { allowRewrites = true }
          }
        }
      } else if (journal.size > config.maxMemorySize.inBytes) {
        log.info("Rotating journal file for '%s' (qsize=%d)", name, queueSize)
        val setCheckpoint = (journal.size + journal.archivedSize > config.maxJournalSize.inBytes)
        rotateJournal(setCheckpoint)
      }
    } else {
      if (allowRewrites &&
          journal.size >= config.defaultJournalSize.inBytes &&
          queueLength == 0) {
        log.info("Rewriting journal file for '%s' (qsize=0)", name)
        rewriteJournal()
        /* KEST 366 - This condition is supposed to be opportunistic and is done frequently
         * with the hope that the journal shrinks to a very small size at the end of this operation
         * as the queue is empty.
         * This assumption is however untrue if there are a large number of open transactions,
         * if after rewrite we end up with a size greater than half the default size,
         * take a break before attempting to rewrite again
         */
        if (journal.size >= (config.defaultJournalSize.inBytes / 2)) {
            disallowRewritesForDelay()
          }
      } else if (allowRewrites &&
                 journal.size + journal.archivedSize > config.maxJournalSize.inBytes &&
                 queueSize < config.maxMemorySize.inBytes) {
        log.info("Rewriting journal file for '%s' (qsize=%d)", name, queueSize)
        rewriteJournal()
        disallowRewritesForDelay()
      } else if (journal.size > config.maxMemorySize.inBytes) {
        /*
         * If the queue is empty, we should First try re-writing -
         * only if that doesn't help, then rotate the journals
         */
        if (queueLength == 0) {
          log.info("Rewriting journal file for '%s' (qsize=0)", name)
          rewriteJournal()
        }

        if (journal.size > config.maxMemorySize.inBytes) {
          log.info("Rotating journal file for '%s' (qsize=%d)", name, queueSize)
          val setCheckpoint = (journal.size + journal.archivedSize > config.maxJournalSize.inBytes)
          rotateJournal(setCheckpoint)
        }
      }
    }
  }

  // for tests.
  def forceRewrite(failPoint: Failpoint) {
    synchronized {
      if (config.keepJournal) {
        log.info("Rewriting journal file for '%s' (qsize=%d)", name, queueSize)
        journal.rewrite(openTransactionIds.map { openTransactions(_) }, queue, failPoint)
        totalRewrites.incr()
      }
    }
  }

  /**
   * Add a value to the end of the in memory queue, and ignore the journal write future.
   */
  def add(value: Array[Byte], expiry: Option[Time], xid: Option[Int], addTime: Time): Boolean = {
    addDurable(value, expiry, xid, addTime) match {
      case Some(f) => true
      case None => false
    } 
  }

  def add(value: Array[Byte]): Boolean = add(value, None, None, Time.now)
  def add(value: Array[Byte], expiry: Option[Time]): Boolean = add(value, expiry, None, Time.now)

  def continue(xid: Int, value: Array[Byte]): Boolean = add(value, None, Some(xid), Time.now)
  def continue(xid: Int, value: Array[Byte], expiry: Option[Time]): Boolean = add(value, expiry, Some(xid), Time.now)

  /**
   * Add a value to the end of the queue, transactionally. If the result is defined, the item has been added to 
   * the in memory queue and the future will be completed when the write is persisted. If the result is not defined 
   * enqueue to in memory queue failed and journal write was not attempted. 
   */
  def addDurable(value: Array[Byte], expiry: Option[Time], xid: Option[Int], addTime: Time): Option[Future[Unit]] = {
    requestSizeMetric.add(value.size)

    // The check is done once per main queue and skipped on each fanout.
    // If a separate whiteListClientIdForEnqueue is specified on the fanout,
    // it is disregarded
    if (!isFanout) {
      config.whiteListClientIdForEnqueue map { whitelistId =>
        ClientId.current match {
          case Some(clientId) => {
            if (!whitelistId.equals(clientId.name)) {
              val error = String.format("ClientId %s is not allowed to enqueue to queue %s", clientId.name, name)
              log.error(error)
              throw new AvailabilityException("add", name, error)
            }
          }
          case None => {
            val error = String.format("ClientId not specified when queue %s only allows a white-listed clientId", name)
            log.warning(error)
            throw new AvailabilityException("add", name, error)
          }
        }
      }
    }
    val futureWrite = synchronized {
      if (closed || value.size > config.maxItemSize.inBytes) return None
      if (config.fanoutOnly && !isFanout) return Some(Future.Done)
      while (queueLength >= config.maxItems || queueSize >= config.maxSize.inBytes) {
        if (!config.discardOldWhenFull) return None
        _remove(false, None)
        totalDiscarded.incr()
        if (config.keepJournal) journal.remove()
        fillReadBehind()
      }
      val item = QItem(addTime, adjustExpiry(Time.now, expiry), value, 0)
      if (config.keepJournal) {
        checkRotateJournal()
        if (!journal.inReadBehind && (queueSize >= config.maxMemorySize.inBytes)) {
          log.info("Dropping to read-behind for queue '%s' (%s)", name, queueSize.bytes.toHuman())
          journal.startReadBehind()
        }
      }
      if (xid != None) {
        openTransactions.remove(xid.get) map { item =>
          config.openTransactionTimeout.map { timeout =>
            transactionExpiryList.remove(item.xid)
          }
        }
      }
      _add(item)
      if (config.keepJournal) {
        xid match {
          case None => journal.add(item)
          case Some(xid) => journal.continue(xid, item)
        }
      } else {
        Future.Done
      }
    }
    addInternalsMetrics()
    waiters.trigger()
    Some(futureWrite)
  }

  def addDurable(value: Array[Byte]): Option[Future[Unit]] = addDurable(value, None, None, Time.now)

  /**
   * Allows us to get more detail about the state of internal datastructures efficiently.
   * We don't care what percentiles or counts mean here, we just want to keep track of how 
   * these data structures look most of the time.
   */
  private[this] def addInternalsMetrics() {  
    txMapSizeMetric.add(openTransactions.size)
    txExpireListSizeMetric.add(transactionExpiryList.size)
    queueSizeMetric.add(queue.size)
  }

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
    config.whiteListClientIdForDequeue map { whitelistId =>
      ClientId.current match {
        case Some(clientId) => {
          if (!whitelistId.equals(clientId.name)) {
            val error = String.format("ClientId %s is not allowed to dequeue from queue %s", clientId.name, name)
            log.error(error)
            throw new AvailabilityException("remove", name, error)
          }
        }
        case None => {
          val error = String.format("ClientId not specified when queue %s only allows a white-listed clientId", name)
          log.warning(error)
          throw new AvailabilityException("remove", name, error)
        }
      }
    }

    val removedItem = synchronized {
      if (closed || paused || queueLength == 0) {
        None
      } else {
        if (transaction) totalTransactions.incr()
        // We time out the oldest read if any, this is done before the remove so that
        // the same checks that are used for live items (expiration etc.) can be applied
        // before redelivery
        timeOutOldestOpenRead()
        val itemOption = _remove(transaction, None)

        // Track the item in the transaction expiry list if the queue has an
        // open transaction timeout
        if (transaction) {
          config.openTransactionTimeout.map { timeout =>
            itemOption map { item =>
              transactionExpiryList.put(item.xid, Time.now + timeout)
            }
          }
        }

        if (config.keepJournal && itemOption.isDefined) {
          if (transaction) journal.removeTentative(itemOption.get.xid) else journal.remove()
          fillReadBehind()
          checkRotateJournal()
        }

        itemOption
      }
    }
    addInternalsMetrics()
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
                            promise: Promise[Option[QItem]]) {
    val item = op
    if (synchronized {
      if (!item.isDefined && !closed && !paused && deadline.isDefined && deadline.get > Time.now) {
        // if we get woken up, try again with the same deadline.
        def onTrigger() = {
          // checking future.isCancelled is a race, but only means that an item may be removed &
          // then un-removed at a higher level if the connection is closed. it's an optimization
          // to let un-acked items get returned before this timeout.
          if (promise.isInterrupted.isDefined) {
            promise.setValue(None)
            waiters.trigger()
          } else {
            // if we get woken up, try again with the same deadline.
            waitOperation(op, startTime, deadline, promise)
          }
        }
        def onTimeout() {
          val msec = (Time.now - startTime).inMilliseconds.toInt
          Stats.addMetric("get_timeout_msec", msec)
          Stats.addMetric("q/" + name + "/get_timeout_msec", msec)
          promise.setValue(None)
        }
        val w = waiters.add(deadline.get, onTrigger, onTimeout)
        promise.setInterruptHandler { case _ =>
          waiters.remove(w)
        }
        false
      } else {
        true
      }
    }) promise.setValue(item)
  }

  def waitRemove(deadline: Option[Time], transaction: Boolean): Future[Option[QItem]] = {
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
    promise map { itemOption =>
      if (itemOption.isDefined) getItemsHit.getAndIncrement() else getItemsMiss.getAndIncrement()
      itemOption
    }
  }

  final def waitPeek(deadline: Option[Time]): Future[Option[QItem]] = {
    val promise = new Promise[Option[QItem]]()
    waitOperation(peek(), Time.now, deadline, promise)
    promise map { itemOption =>
      if (itemOption.isDefined) getItemsHit.getAndIncrement() else getItemsMiss.getAndIncrement()
      itemOption
    }
  }

  def evictWaiters() {
    synchronized {
      waiters.evictAll()
    }
  }

  /**
   * Return a transactionally-removed item to the queue. This is a rolled-
   * back transaction.
   */
  def unremove(xid: Int) {
    synchronized {
      if (!closed) {
        _unremove(xid) match {
          case Some(_) => {
            if (config.keepJournal) journal.unremove(xid)
            totalCanceledTransactions.incr()
            config.openTransactionTimeout.map { timeout =>
              transactionExpiryList.remove(xid)
            }
          }
          case None =>
            log.warning("Queue %s: Trying to abort a transaction (%d) that was not open", name, xid)

        }
        waiters.trigger()
      }
    }
  }

  def confirmRemove(xid: Int) {
    synchronized {
      if (!closed) {
        openTransactions.remove(xid) match {
          case Some(_) => {
            if (config.keepJournal) journal.confirmRemove(xid)
            config.openTransactionTimeout.map { timeout =>
              transactionExpiryList.remove(xid)
            }
          }
          case None => {
            log.warning("Queue %s: Trying to commit a transaction (%d) that was not open", name, xid)
          }
        }
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
    close(false)
  }

    /**
   * Close the queue's journal file. Not safe to call on an active queue.
   */
  def close(gracefulShutdown: Boolean) {
    synchronized {
      if ((gracefulShutdown) && (0 == queueLength)) {
        log.info("Rewriting journal file during graceful shutdown for '%s' (qsize=0)", name)
        rewriteJournal()
      }
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
      queueLength = 0
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
    Stats.removeCounter(statNamed("get_items_hit"))
    Stats.removeCounter(statNamed("get_items_miss"))
    Stats.removeCounter(statNamed("put_bytes"))
    Stats.removeCounter(statNamed("put_items"))
    Stats.removeCounter(statNamed("expired_items"))
    Stats.removeCounter(statNamed("transactions"))
    Stats.removeCounter(statNamed("canceled_transactions"))
    Stats.removeCounter(statNamed("discarded"))
    Stats.removeCounter(statNamed("total_flushes"))
    Stats.removeCounter(statNamed("journal_rewrites"))
    Stats.removeCounter(statNamed("journal_rotations"))
    Stats.clearGauge(statNamed("items"))
    Stats.clearGauge(statNamed("max_items"))
    Stats.clearGauge(statNamed("bytes"))
    Stats.clearGauge(statNamed("max_bytes"))
    Stats.clearGauge(statNamed("journal_size"))
    Stats.clearGauge(statNamed("mem_items"))
    Stats.clearGauge(statNamed("mem_bytes"))
    Stats.clearGauge(statNamed("age_msec"))
    Stats.clearGauge(statNamed("waiters"))
    Stats.clearGauge(statNamed("open_transactions"))
    Stats.clearGauge(statNamed("create_time"))
    Stats.removeMetric(statNamed("journal_rewrite_usec"))
    Stats.removeMetric(statNamed("journal_rotation_usec"))
    Stats.removeMetric(statNamed("set_latency_usec")) // see KestrelHandler
    Stats.removeMetric(statNamed("get_timeout_msec"))
    Stats.removeMetric(statNamed("delivery_latency_msec"))
    Stats.removeMetric(statNamed("get_hit_latency_usec"))
    Stats.removeMetric(statNamed("get_miss_latency_usec"))
    Stats.removeMetric(statNamed("fill_readbehind_usec"))
    Stats.removeMetric(statNamed("tx_map_size"))
    Stats.removeMetric(statNamed("tx_expire_list_size"))
    Stats.removeMetric(statNamed("queue_size"))
    Stats.removeMetric(statNamed("request_size"))
  }

  private final def nextXid(): Int = {
    do {
      xidCounter += 1
    } while ((openTransactions contains xidCounter) || (xidCounter == 0))
    xidCounter
  }

  private final def fillReadBehind() {
    val elapsed = Stopwatch.start()

    // if we're in read-behind mode, scan forward in the journal to keep memory as full as
    // possible. this amortizes the disk overhead across all reads.
    while (config.keepJournal && journal.inReadBehind && _memoryBytes < config.maxMemorySize.inBytes) {
      journal.fillReadBehind { item =>
        queue.add(item)
        _memoryBytes += item.data.length
      } { checkpoint =>
        log.info("Rewriting journal file from checkpoint for '%s' (qsize=%d)", name, queueSize)
        journal.startPack(checkpoint, openTransactions.values.toList, queue.toList)
      }
      if (!journal.inReadBehind) {
        log.info("Coming out of read-behind for queue '%s'", name)
      }
    }

    fillReadBehindMetric.add(elapsed().inMicroseconds.toInt)
  }

  def replayJournal() {
    if (!config.keepJournal) return

    val sw = Stopwatch.start()
    log.info("Replaying transaction journal for '%s'", name)
    xidCounter = 0

    journal.replay( true , {
      case JournalItem.Add(item) =>
        _add(item)
        // when processing the journal, this has to happen after:
        if (!journal.inReadBehind && queueSize >= config.maxMemorySize.inBytes) {
          log.info("Dropping to read-behind for queue '%s' (%d bytes)", name, queueSize)
          journal.startReadBehind()
        }
      case JournalItem.Remove =>
        _remove(false, None)
        journal.notifyRemoveDuringReplay()
      case JournalItem.RemoveTentative(xid) =>
        _remove(true, Some(xid))
        xidCounter = xid
      case JournalItem.SavedXid(xid) => xidCounter = xid
      case JournalItem.Unremove(xid) => _unremove(xid)
      case JournalItem.ConfirmRemove(xid) =>
        openTransactions.remove(xid)
        journal.notifyRemoveDuringReplay()
      case JournalItem.Continue(item, xid) =>
        openTransactions.remove(xid)
        journal.notifyRemoveDuringReplay()
        _add(item)
      case x => log.error("Unexpected item in journal: %s", x)
    })

    log.info("Finished scanning the transaction journal for '%s' (%d items, %d bytes) xid=%d, removes=%d", name, queueLength,
             journal.size, xidCounter, journal.removesSinceReadBehind)
    journal.open()

    // now, any unfinished transactions must be backed out.
    for (xid <- openTransactionIds) {
      journal.unremove(xid)
      _unremove(xid)
    }

    log.info("Finished replaying the transaction journal for '%s' in %d milliseconds", name, sw().inMilliseconds.toInt)

  }

  /**
   * Retrieves the oldest add time from the queue. This is only meant to be an estimate, which is
   * why we don't synchronize access to the queue.
   *
   * This method is expensive and will return 0 if there are more than 10000 items.
   * If there are no items in the queue, it will return current timestamp.
   *
   * @return oldest add time in milliseconds
   */
  def getOldestAddTime: Long = {
    if (queueLength <= 10000)
      queue.foldLeft(Time.now.inMilliseconds)((m, i) => scala.math.min(m, i.addTime.inMilliseconds))
    else
      0
  }

  //  -----  internal implementations

  private def _add(item: QItem) {
    discardExpired()
    if (!journal.inReadBehind) {
      queue.add(item)
      _memoryBytes += item.data.length
    }
    putItems.getAndIncrement()
    putBytes.getAndAdd(item.data.length)
    queueSize += item.data.length
    queueLength += 1
  }

  private def _peek(): Option[QItem] = {
    discardExpired()
    if (queue.isEmpty) None else Some(queue.peekFirst)
  }

  private def _remove(transaction: Boolean, xid: Option[Int]): Option[QItem] = {
    discardExpired()
    if (queue.isEmpty) return None

    val now = Time.now
    val item = queue.remove()
    val len = item.data.length
    queueSize -= len
    _memoryBytes -= len
    queueLength -= 1
    if (journal.isReplaying) {
      fillReadBehind()
    }
    _currentAge = item.addTime
    if (transaction) {
      item.xid = xid.getOrElse { nextXid() }
      openTransactions(item.xid) = item
    }
    Some(item)
  }

  final def discardExpired(limit: Int = config.maxExpireSweep): Int = {
    val itemsToRemove = synchronized {
      var continue = true
      val hasLimit = limit < Int.MaxValue
      val toRemove = new mutable.ListBuffer[QItem]
      while (continue) {
        if (queue.isEmpty || hasLimit && toRemove.length >= limit || journal.isReplaying) {
          continue = false
        } else {
          val realExpiry = adjustExpiry(queue.peekFirst.addTime, queue.peekFirst.expiry)
          if (realExpiry.isDefined && realExpiry.get < Time.now) {
            totalExpired.incr()
            val item = queue.remove()
            val len = item.data.length
            queueSize -= len
            _memoryBytes -= len
            queueLength -= 1
            if (config.keepJournal) journal.remove()
            if (queue.isEmpty) {
              fillReadBehind()
            }
            toRemove += item
          } else {
            continue = false
          }
        }
      }
      if (toRemove.length > 0) {
        fillReadBehind()
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
      queue.addFirst(item)
      _memoryBytes += item.data.length
    }
  }

  private def timeOutOldestOpenRead() {
    config.openTransactionTimeout.flatMap { timeout =>
      if (transactionExpiryList.size > 0) {
        Option(transactionExpiryList.entrySet.iterator.next)
      } else {
        None
      }
    } map { txEntry =>
      if (txEntry.getValue < Time.now) {
        unremove(txEntry.getKey)
      }
    }
  }
}
