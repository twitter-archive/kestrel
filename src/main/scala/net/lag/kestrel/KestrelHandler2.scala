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

/**
 * Common implementations of kestrel commands that don't depend on which protocol you're using.
 */
class KestrelHandler2(
  val queues: QueueCollection,
  val maxOpenTransactions: Int,
  clientDescription: => String,
  sessionId: Int
) {
  private val log = Logger.get(getClass.getName)

  object pendingReliableReads {  // pending Random Access Transactions.
                                  // used for syn, ack, fail
    private val reliableReadIds = new mutable.HashMap[String, mutable.HashSet[Int]] {
      override def default(key: String) = {
        val rv = new mutable.HashSet[Int]
        this(key) = rv
        rv
      }
    }

    def remove(name: String, xid: Int): Boolean = synchronized {
      reliableReadIds(name).remove(xid)
    }

    def add(name: String, xid: Int) = synchronized {
      reliableReadIds(name) += xid
    }

    def size(name: String): Int = synchronized { reliableReadIds(name).size }

    def cancelAll() {
      synchronized {
        reliableReadIds.foreach { case (name, xids) =>
          xids.foreach { xid => queues.unremove(name, xid) }
        }
        reliableReadIds.clear()
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

  def abortReliableRead(key: String, xid: Int): Boolean = {
    pendingReliableReads.remove(key, xid) match {
      case true =>
        log.debug("fail -> q=%s, xid=%d", key, xid)
        queues.unremove(key, xid)
        true
      case false =>
        log.warning("Attempt to fail a non-existent open on '%s [xid=%d]' (sid %d, %s)",
                    key, xid, sessionId, clientDescription)
        false
    }
  }

  def confirmReliableRead(key: String, xid: Int): Boolean = {
    pendingReliableReads.remove(key, xid) match {
      case true =>
        log.debug("ack -> q=%s, xid=%d", key, xid)
        queues.confirmRemove(key, xid)
        true
      case false =>
        log.warning("Attempt to ack a non-existent open on '%s [xid=%d]' (sid %d, %s)",
                    key, xid, sessionId, clientDescription)
        false
    }
  }

  def getItem(key: String, timeout: Option[Time], open: Boolean): Future[Option[QItem]] = {
    if (pendingReliableReads.size(key) >= maxOpenTransactions) {
      log.warning("Attempt to open too many transactions on '%s' (sid %d, %s)", key, sessionId,
                  clientDescription)
      throw TooManyOpenTransactionsException
    }

    log.debug("get -> q=%s timeout=%s open?=%s", key, timeout, open)
    Stats.incr("cmd_get")

    queues.remove(key, timeout, open, false).map { itemOption =>
      itemOption.foreach { item =>
        log.debug("get <- %s", item)
        if (open) pendingReliableReads.add(key, item.xid)
      }
      itemOption
    }
  }

  def abortAnyTransaction() {
    pendingReliableReads.cancelAll()
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
