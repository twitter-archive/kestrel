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

import net.lag.configgy.Config
import net.lag.logging.Logger

/**
 * Common implementations of kestrel commands that don't depend on which protocol you're using.
 */
abstract class KestrelHandler(val config: Config) {
  private val log = Logger.get

  protected val sessionID = KestrelStats.sessionID.incr
  protected var pendingTransaction: Option[(String, Int)] = None

  // used internally to indicate a client error: tried to close a transaction on the wrong queue.
  protected class MismatchedQueueException extends Exception

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
    Kestrel.queues.queueNames.foreach { qName => Kestrel.queues.flush(qName) }
  }

  protected def abortTransaction(key: String): Boolean = {
    pendingTransaction match {
      case None =>
        log.warning("Attempt to abort a non-existent transaction on '%s' (sid %d, %s)",
                    key, sessionID, clientDescription)
        false
      case Some((qname, xid)) =>
        if (qname != key) {
          log.warning("Attempt to close a transaction on the wrong queue '%s' (sid %d, %s)",
                      key, sessionID, clientDescription)
          throw new MismatchedQueueException
        } else {
          log.debug("abort -> q=%s", key)
          Kestrel.queues.unremove(qname, xid)
          pendingTransaction = None
        }
        true
    }
  }

  // returns true if a transaction was actually closed.
  protected def closeTransaction(key: String): Boolean = {
    pendingTransaction match {
      case None =>
        false
      case Some((qname, xid)) =>
        if (qname != key) {
          log.warning("Attempt to close a transaction on the wrong queue '%s' (sid %d, %s)",
                      key, sessionID, clientDescription)
          throw new MismatchedQueueException
        } else {
          log.debug("confirm -> q=%s", key)
          Kestrel.queues.confirmRemove(qname, xid)
          pendingTransaction = None
        }
        true
    }
  }

  protected def getItem(key: String, timeout: Int, opening: Boolean, peeking: Boolean)(f: Option[QItem] => Unit) {
    log.debug("get -> q=%s t=%d open=%s peek=%s", key, timeout, opening, peeking)
    if (peeking) {
      KestrelStats.peekRequests.incr
    } else {
      KestrelStats.getRequests.incr
    }
    Kestrel.queues.remove(key, timeout, opening, peeking) {
      case None =>
        f(None)
      case Some(item) =>
        log.debug("get <- %s", item)
        if (opening) pendingTransaction = Some((key, item.xid))
        f(Some(item))
    }
  }

  protected def abortAnyTransaction() = {
    pendingTransaction.foreach { case (qname, xid) => Kestrel.queues.unremove(qname, xid) }
    pendingTransaction = None
  }

  protected def setItem(key: String, flags: Int, expiry: Int, data: Array[Byte]) = {
    log.debug("set -> q=%s flags=%d expiry=%d size=%d", key, flags, expiry, data.length)
    KestrelStats.setRequests.incr()
    Kestrel.queues.add(key, data, expiry)
  }

  protected def flush(key: String) = {
    log.debug("flush -> q=%s", key)
    Kestrel.queues.flush(key)
  }

  protected def rollJournal(key: String) {
    log.debug("roll -> q=%s", key)
    Kestrel.queues.rollJournal(key)
  }

  protected def delete(key: String) = {
    log.debug("delete -> q=%s", key)
    Kestrel.queues.delete(key)
  }

  protected def flushExpired(key: String) = {
    log.debug("flush_expired -> q=%s", key)
    Kestrel.queues.flushExpired(key)
  }

  protected def shutdown() = {
    Kestrel.shutdown
  }
}
