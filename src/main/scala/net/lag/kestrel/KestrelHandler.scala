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

class TooManyOpenTransactionsException extends Exception("Too many open transactions.")
object TooManyOpenTransactionsException extends TooManyOpenTransactionsException

/**
 * Common implementations of kestrel commands that don't depend on which protocol you're using.
 */
class KestrelHandler(
  val queues: QueueCollection,
  val maxOpenTransactions: Int,
  clientDescription: => String,
  sessionId: Int
) {
  private val log = Logger.get(getClass.getName)

  object pendingRATransactions {  // pending Random Access Transactions.
                                  // used for syn, ack, fail
    private val transactions = new mutable.HashMap[String, mutable.HashSet[Int]] {
      override def default(key: String) = {
        val rv = new mutable.HashSet[Int]
        this(key) = rv
        rv
      }
    }

    def remove(name: String, xid: Int): Boolean = synchronized {
      transactions(name).remove(xid)
    }

    def add(name: String, xid: Int) = synchronized {
      transactions(name) += xid
    }

    def size(name: String): Int = synchronized { transactions(name).size }

    def cancelAll() {
      synchronized {
        transactions.foreach { case (name, xids) =>
          xids.foreach { xid => queues.unremove(name, xid) }
        }
        transactions.clear()
      }
    }
  }

  object pendingTransactions {
    private val transactions = new mutable.HashMap[String, mutable.ListBuffer[Int]] {
      override def default(key: String) = {
        val rv = new mutable.ListBuffer[Int]
        this(key) = rv
        rv
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

    def cancelAll() {
      synchronized {
        transactions.foreach { case (name, xids) =>
          xids.foreach { xid => queues.unremove(name, xid) }
        }
        transactions.clear()
      }
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

  // usually called when netty sends a disconnect signal.
  def finish() {
    log.debug("End of session %d", sessionId)
    abortAnyTransaction()
    Kestrel.sessions.decrementAndGet()
  }

  def flushAllQueues() {
    queues.queueNames.foreach { qName => queues.flush(qName) }
  }

  def failTransaction(key: String, xid: Int): Boolean = {
    pendingRATransactions.remove(key, xid) match {
      case true =>
        log.debug("fail -> q=%s, xid=%d", key, xid)
        queues.unremove(key, xid)
        true
      case false =>
        log.warning("Attempt to fail a non-existent transaction on '%s [xid=%d]' (sid %d, %s)",
                    key, xid, sessionId, clientDescription)
        false
    }
  }

  def ackTransaction(key: String, xid: Int): Boolean = {
    pendingRATransactions.remove(key, xid) match {
      case true =>
        log.debug("ack -> q=%s, xid=%d", key, xid)
        queues.confirmRemove(key, xid)
        true
      case false =>
        log.warning("Attempt to ack a non-existent transaction on '%s [xid=%d]' (sid %d, %s)",
                    key, xid, sessionId, clientDescription)
        false
    }
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
    // TODO: Not implemented for pendingRATransactions
    xids.size
  }

  // will do a continuous transactional fetch on a queue until time runs out or transactions are full.
  final def monitorUntil(key: String, timeLimit: Time)(f: Option[QItem] => Unit) {
    if (timeLimit <= Time.now
    || (pendingTransactions.size(key) + pendingRATransactions.size(key)) >= maxOpenTransactions) {
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

  def getItemSyn(key: String, timeout: Option[Time]): Future[Option[QItem]] = {
    if (pendingTransactions.size(key) + pendingRATransactions.size(key) >= maxOpenTransactions) {
      log.warning("Attempt to open too many transactions on '%s' (sid %d, %s)", key, sessionId,
                  clientDescription)
      throw TooManyOpenTransactionsException
    }

    log.debug("get -> q=%s t=%s syn=true", key, timeout)
    Stats.incr("cmd_get")

    queues.remove(key, timeout, true, false).map { itemOption =>
      itemOption.foreach { item =>
        log.debug("get <- %s", item)
        pendingRATransactions.add(key, item.xid)
      }
      itemOption
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
    queues.remove(key, timeout, opening, peeking).map { itemOption =>
      itemOption.foreach { item =>
        log.debug("get <- %s", item)
        if (opening) pendingTransactions.add(key, item.xid)
      }
      itemOption
    }
  }

  def abortAnyTransaction() {
    pendingTransactions.cancelAll()
    pendingRATransactions.cancelAll()
  }

  def setItem(key: String, flags: Int, expiry: Option[Time], data: Array[Byte]) = {
    log.debug("set -> q=%s flags=%d expiry=%s size=%d", key, flags, expiry, data.length)
    Stats.incr("cmd_set")
    queues.add(key, data, expiry)
  }

  def flush(key: String) {
    log.debug("flush -> q=%s", key)
    queues.flush(key)
  }

  def delete(key: String) {
    log.debug("delete -> q=%s", key)
    queues.delete(key)
  }

  def flushExpired(key: String) = {
    log.debug("flush_expired -> q=%s", key)
    queues.flushExpired(key)
  }

  def shutdown() {
    BackgroundProcess {
      Thread.sleep(100)
      ServiceTracker.shutdown()
    }
  }
}
