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
import net.lag.configgy.Config
import net.lag.logging.Logger

/**
 * Common implementations of kestrel commands that don't depend on which protocol you're using.
 */
abstract class KestrelHandler(val config: Config, val queues: QueueCollection) {
  private val log = Logger.get
  val maxOpenTransactions = config.getInt("max_open_transactions", 1)

  val sessionID = KestrelStats.sessionID.incr
  val pendingTransactions = new mutable.HashMap[String, mutable.ListBuffer[Int]] {
    override def default(key: String) = {
      this(key) = new mutable.ListBuffer[Int]
      this(key)
    }
  }

  KestrelStats.sessions.incr()
  KestrelStats.totalConnections.incr()

  protected def clientDescription: String

  // usually called when mina sends a disconnect signal.
  protected def finish() {
    log.debug("End of session %d", sessionID)
    abortAnyTransaction()
    KestrelStats.sessions.decr()
  }

  protected def flushAllQueues() {
    queues.queueNames.foreach { qName => queues.flush(qName) }
  }

  def abortTransaction(key: String): Boolean = {
    pendingTransactions(key).headOption match {
      case None =>
        log.warning("Attempt to abort a non-existent transaction on '%s' (sid %d, %s)",
                    key, sessionID, clientDescription)
        false
      case Some(xid) =>
        log.debug("abort -> q=%s", key)
        queues.unremove(key, xid)
        pendingTransactions(key).remove(0)
        true
    }
  }

  // returns true if a transaction was actually closed.
  def closeTransaction(key: String): Boolean = {
    pendingTransactions(key).headOption match {
      case None =>
        false
      case Some(xid) =>
        log.debug("confirm -> q=%s", key)
        queues.confirmRemove(key, xid)
        pendingTransactions(key).remove(0)
        true
    }
  }

  def getItem(key: String, timeout: Int, opening: Boolean, peeking: Boolean)(f: Option[QItem] => Unit) {
    log.debug("get -> q=%s t=%d open=%s peek=%s", key, timeout, opening, peeking)
    if (peeking) {
      KestrelStats.peekRequests.incr
    } else {
      KestrelStats.getRequests.incr
    }
    queues.remove(key, timeout, opening, peeking) {
      case None =>
        f(None)
      case Some(item) =>
        log.debug("get <- %s", item)
        if (opening) pendingTransactions(key) += item.xid
        f(Some(item))
    }
  }

  protected def abortAnyTransaction() = {
    pendingTransactions.foreach { case (qname, xidList) =>
      xidList.foreach { xid => queues.unremove(qname, xid) }
    }
    pendingTransactions.clear()
  }

  def setItem(key: String, flags: Int, expiry: Int, data: Array[Byte]) = {
    log.debug("set -> q=%s flags=%d expiry=%d size=%d", key, flags, expiry, data.length)
    KestrelStats.setRequests.incr()
    queues.add(key, data, expiry)
  }

  protected def flush(key: String) = {
    log.debug("flush -> q=%s", key)
    queues.flush(key)
  }

  protected def rollJournal(key: String) {
    log.debug("roll -> q=%s", key)
    queues.rollJournal(key)
  }

  protected def delete(key: String) = {
    log.debug("delete -> q=%s", key)
    queues.delete(key)
  }

  protected def flushExpired(key: String) = {
    log.debug("flush_expired -> q=%s", key)
    queues.flushExpired(key)
  }

  protected def shutdown() = {
    Kestrel.shutdown
  }
}
