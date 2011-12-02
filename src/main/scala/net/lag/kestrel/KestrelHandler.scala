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

import scala.collection.mutable
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.ostrich.admin.{BackgroundProcess, ServiceTracker}
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Future, Duration, Time}
import java.util.concurrent.atomic.AtomicBoolean

class TooManyOpenTransactionsException extends Exception("Too many open transactions.")
object TooManyOpenTransactionsException extends TooManyOpenTransactionsException

/**
 * Common implementations of kestrel commands that don't depend on which protocol you're using.
 */
abstract class KestrelHandler(val queues: QueueCollection, val maxOpenTransactions: Int) {
  private val log = Logger.get(getClass.getName)

  val sessionId = Kestrel.sessionId.incrementAndGet()
  val finished = new AtomicBoolean(false)
  @volatile var waitingFor: Option[Future[Option[QItem]]] = None

  object pendingTransactions {
    private var transactions = createMap()

    private def createMap() = {
      new mutable.HashMap[String, mutable.ListBuffer[Int]] {
        override def default(key: String) = {
          val rv = new mutable.ListBuffer[Int]
          this(key) = rv
          rv
        }
      }
    }

    def pop(name: String): Option[Int] = synchronized {
      val rv = transactions(name).headOption
      rv.foreach { x => transactions(name).remove(0) }
      rv
    }

    def popN(name: String, count: Int): Option[Seq[Int]] = synchronized {
      if (transactions(name).size < count) {
        None
      } else {
        Some((0 until count).map { x => transactions(name).remove(0) })
      }
    }

    def add(name: String, xid: Int) = synchronized {
      transactions(name) += xid
    }

    def size(name: String): Int = synchronized { transactions(name).size }

    def peek(name: String): List[Int] = synchronized { transactions(name).toList }

    def cancelAll(): Int = {
      var count = 0
      synchronized {
        val currentTransactions = transactions
        transactions = createMap()
        currentTransactions
      }.foreach { case (name, xids) =>
        count += xids.size
        xids.foreach { xid => queues.unremove(name, xid) }
      }
      count
    }

    def popAll(name: String): Seq[Int] = {
      synchronized {
        val xids = transactions(name).toArray
        transactions(name).clear()
        xids
      }
    }
  }

  Kestrel.sessions.incrementAndGet()
  Stats.incr("total_connections")

  protected def clientDescription: String

  // usually called when netty sends a disconnect signal.
  protected def finish() {
    abortAnyTransaction()
    waitingFor.foreach { w =>
      w.cancel()
      Stats.incr("cmd_get_timeout_dropped")
    }

    if (finished.getAndSet(true) == false) {
      log.debug("End of session %d", sessionId)
      Kestrel.sessions.decrementAndGet()
    }
  }

  protected def flushAllQueues() {
    queues.queueNames.foreach { qName => queues.flush(qName) }
  }

  // returns true if a transaction was actually aborted.
  def abortTransaction(key: String): Boolean = {
    pendingTransactions.pop(key) match {
      case None =>
        log.warning("Attempt to abort a non-existent transaction on '%s' (sid %d, %s)",
                    key, sessionId, clientDescription)
        false
      case Some(xid) =>
        log.debug("abort -> q=%s", key)
        queues.unremove(key, xid)
        true
    }
  }

  // returns true if a transaction was actually closed.
  def closeTransaction(key: String): Boolean = {
    pendingTransactions.pop(key) match {
      case None =>
        false
      case Some(xid) =>
        log.debug("confirm -> q=%s", key)
        queues.confirmRemove(key, xid)
        true
    }
  }

  def closeTransactions(key: String, count: Int): Boolean = {
    pendingTransactions.popN(key, count) match {
      case None =>
        false
      case Some(xids) =>
        xids.foreach { xid => queues.confirmRemove(key, xid) }
        true
    }
  }

  def closeAllTransactions(key: String): Int = {
    val xids = pendingTransactions.popAll(key)
    xids.foreach { xid => queues.confirmRemove(key, xid) }
    xids.size
  }

  // will do a continuous transactional fetch on a queue until time runs out or transactions are full.
  final def monitorUntil(key: String, timeLimit: Time)(f: Option[QItem] => Unit) {
    if (timeLimit <= Time.now || pendingTransactions.size(key) >= maxOpenTransactions) {
      f(None)
    } else {
      queues.remove(key, Some(timeLimit), true, false).onSuccess {
        case None =>
          f(None)
        case x @ Some(item) =>
          pendingTransactions.add(key, item.xid)
          f(x)
          monitorUntil(key, timeLimit)(f)
      }
    }
  }

  def getItem(key: String, timeout: Option[Time], opening: Boolean, peeking: Boolean): Future[Option[QItem]] = {
    if (opening && pendingTransactions.size(key) >= maxOpenTransactions) {
      log.warning("Attempt to open too many transactions on '%s' (sid %d, %s)", key, sessionId,
                  clientDescription)
      throw TooManyOpenTransactionsException
    }

    log.debug("get -> q=%s t=%s open=%s peek=%s", key, timeout, opening, peeking)
    if (peeking) {
      Stats.incr("cmd_peek")
    } else {
      Stats.incr("cmd_get")
    }
    val startTime = Time.now
    val future = queues.remove(key, timeout, opening, peeking)
    waitingFor = Some(future)
    future.map { itemOption =>
      waitingFor = None
      itemOption.foreach { item =>
        log.debug("get <- %s", item)
        if (opening) pendingTransactions.add(key, item.xid)
      }
      itemOption
    }
  }

  protected def abortAnyTransaction() = {
    Stats.incr("cmd_get_open_dropped", pendingTransactions.cancelAll())
  }

  def setItem(key: String, flags: Int, expiry: Option[Time], data: Array[Byte]) = {
    log.debug("set -> q=%s flags=%d expiry=%s size=%d", key, flags, expiry, data.length)
    Stats.incr("cmd_set")
    val (rv, nsec) = Duration.inNanoseconds {
      queues.add(key, data, expiry, Time.now)
    }
    Stats.addMetric("set_latency_usec", nsec.inMicroseconds.toInt)
    Stats.addMetric("q/" + key + "/set_latency_usec", nsec.inMicroseconds.toInt)
    rv
  }

  protected def flush(key: String) {
    log.debug("flush -> q=%s", key)
    queues.flush(key)
  }

  protected def delete(key: String) {
    log.debug("delete -> q=%s", key)
    queues.delete(key)
  }

  protected def flushExpired(key: String) = {
    log.debug("flush_expired -> q=%s", key)
    queues.flushExpired(key)
  }

  protected def shutdown() = {
    BackgroundProcess {
      Thread.sleep(100)
      ServiceTracker.shutdown()
    }
  }
}
