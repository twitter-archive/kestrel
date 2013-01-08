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

import com.twitter.conversions.time._
import com.twitter.logging.{Level, Logger}
import com.twitter.ostrich.admin.{BackgroundProcess, ServiceTracker}
import com.twitter.ostrich.stats.Stats
import com.twitter.util._
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.collection.Set

class TooManyOpenReadsException extends Exception("Too many open reads.")
object TooManyOpenReadsException extends TooManyOpenReadsException

class ServerStatusNotConfiguredException
extends Exception("Server status not configured.")

class AvailabilityException(op: String)
extends Exception("Server not available for operation %s".format(op))

trait SimplePendingReads {
  def queues: QueueCollection
  protected def log: Logger
  def sessionId: Int
  def clientDescription: () => String

  object pendingReads {
    private val reads = new mutable.HashMap[String, ItemIdList] {
      override def default(key: String) = {
        val rv = new ItemIdList()
        this(key) = rv
        rv
      }
    }

    def pop(name: String): Option[Int] = synchronized { reads(name).pop() }
    def popN(name: String, count: Int): Seq[Int] = synchronized { reads(name).pop(count) }
    def add(name: String, xid: Int) = synchronized { reads(name) add xid }
    def size(name: String): Int = synchronized { reads(name).size }
    def popAll(name: String): Seq[Int] = synchronized { reads(name).popAll() }
    def peek(name: String): Seq[Int] = synchronized { reads(name).peek() }
    def remove(name: String, ids: Set[Int]): Set[Int] = synchronized { reads(name).remove(ids) }

    def cancelAll(): Int = {
      var count = 0
      synchronized {
        val current = reads.clone()
        reads.clear()
        current
      }.foreach { case (name, xids) =>
        val ids = xids.popAll()
        count += ids.size
        ids.foreach { id => queues.unremove(name, id) }
      }
      count
    }
  }

  // returns true if a read was actually aborted.
  def abortRead(key: String): Boolean = {
    pendingReads.pop(key) match {
      case None =>
        log.warning("Attempt to abort a non-existent read on '%s' (sid %d, %s)",
                    key, sessionId, clientDescription)
        false
      case Some(xid) =>
        log.debug("abort -> q=%s", key)
        queues.unremove(key, xid)
        true
    }
  }

  // returns true if a read was actually closed.
  def closeRead(key: String): Boolean = {
    pendingReads.pop(key) match {
      case None =>
        false
      case Some(xid) =>
        log.debug("confirm -> q=%s", key)
        queues.confirmRemove(key, xid)
        true
    }
  }

  def closeReads(key: String, count: Int): Boolean = {
    val xids = pendingReads.popN(key, count)
    xids.foreach { xid => queues.confirmRemove(key, xid) }
    xids.size > 0
  }

  def closeAllReads(key: String): Int = {
    val xids = pendingReads.popAll(key)
    xids.foreach { xid => queues.confirmRemove(key, xid) }
    xids.size
  }

  def countPendingReads(key: String) = pendingReads.size(key)
  def addPendingRead(key: String, xid: Int): Option[Long] = {
    pendingReads.add(key, xid)
    None
  }
  def cancelAllPendingReads() = pendingReads.cancelAll()
}

/**
 * Common implementations of kestrel commands that don't depend on which protocol you're using.
 */
abstract class KestrelHandler(
  val queues: QueueCollection,
  val maxOpenReads: Int,
  val clientDescription: () => String,
  val sessionId: Int,
  val serverStatus: Option[ServerStatus]
) {
  protected val log = Logger.get(getClass.getName)

  val finished = new AtomicBoolean(false)
  @volatile var waitingFor: Option[Future[Option[QItem]]] = None

  Kestrel.sessions.incrementAndGet()
  Stats.incr("total_connections")

  if (Kestrel.traceSessions) {
    log.info("New session %d from %s", sessionId, clientDescription)
  }

  // called exactly once by finagle when the session ends.
  def finish() {
    abortAnyOpenRead(Kestrel.traceSessions)
    waitingFor.foreach { w =>
      w.cancel()
      Stats.incr("cmd_get_timeout_dropped")
    }

    if (Kestrel.traceSessions) {
      log.info("End of session %d", sessionId)
    } else {
      log.debug("End of session %d", sessionId)
    }

    Kestrel.sessions.decrementAndGet()
  }

  def flushAllQueues() {
    checkBlockWrites("flushAll", "<all>")
    queues.queueNames.foreach { qName => queues.flush(qName, Some(clientDescription)) }
  }

  protected def countPendingReads(key: String): Int
  protected def addPendingRead(key: String, xid: Int): Option[Long]
  protected def cancelAllPendingReads(): Int

  // will do a continuous fetch on a queue until time runs out or read buffer is full.
  final def monitorUntil(key: String, timeLimit: Option[Time], maxItems: Int, opening: Boolean)(f: (Option[QItem], Option[Long]) => Unit) {
    checkBlockReads("monitorUntil", key)

    log.debug("monitor -> q=%s t=%s max=%d open=%s", key, timeLimit, maxItems, opening)
    Stats.incr("cmd_monitor")

    def monitorLoop(maxItems: Int) {
      if (safeCheckBlockReads) {
        f(None, None)
        return
      }

      log.debug("monitor loop -> q=%s t=%s max=%d open=%s", key, timeLimit, maxItems, opening)
      if (maxItems == 0 || (timeLimit.isDefined && timeLimit.get <= Time.now) || countPendingReads(key) >= maxOpenReads) {
        f(None, None)
      } else {
        Stats.incr("cmd_monitor_get")
        queues.remove(key, timeLimit, opening, false, Some(clientDescription)).onSuccess {
          case None =>
            f(None, None)
          case x @ Some(item) =>
            val xidContext = if (opening) addPendingRead(key, item.xid) else None
            f(x, xidContext)
            monitorLoop(maxItems - 1)
        }
      }
    }

    monitorLoop(maxItems)
  }

  def getItem(key: String, timeout: Option[Time], opening: Boolean, peeking: Boolean): Future[Option[QItem]] = {
    checkBlockReads("getItem", key)

    if (opening && countPendingReads(key) >= maxOpenReads) {
      log.warning("Attempt to open too many reads on '%s' (sid %d, %s)", key, sessionId,
                  clientDescription)
      throw TooManyOpenReadsException
    }

    log.debug("get -> q=%s t=%s open=%s peek=%s", key, timeout, opening, peeking)
    if (peeking) {
      Stats.incr("cmd_peek")
    } else {
      Stats.incr("cmd_get")
    }
    val startTime = Time.now
    val future = queues.remove(key, timeout, opening, peeking, Some(clientDescription))
    waitingFor = Some(future)
    future.map { itemOption =>
      waitingFor = None
      itemOption.foreach { item =>
        log.debug("get <- %s", item)
        if (opening) addPendingRead(key, item.xid)
      }
      itemOption
    }
    future.onCancellation {
      // if the connection is closed, pre-emptively return un-acked items.
      abortAnyOpenRead(Kestrel.traceSessions)
    }
    future
  }

  def abortAnyOpenRead(trace: Boolean) {
    val abortedReads = cancelAllPendingReads();
    Stats.incr("cmd_get_open_dropped", abortedReads)
    if (trace) {
      log.info("Aborted %d pending reads", abortedReads);
    }
  }

  def setItem(key: String, flags: Int, expiry: Option[Time], data: Array[Byte]) = {
    checkBlockWrites("setItem", key)
    log.debug("set -> q=%s flags=%d expiry=%s size=%d", key, flags, expiry, data.length)
    Stats.incr("cmd_set")
    val (rv, nsec) = Duration.inNanoseconds {
      queues.add(key, data, expiry, Time.now, Some(clientDescription))
    }
    Stats.addMetric("set_latency_usec", nsec.inMicroseconds.toInt)
    Stats.addMetric("q/" + key + "/set_latency_usec", nsec.inMicroseconds.toInt)
    rv
  }

  def flush(key: String) {
    checkBlockWrites("flush", key)
    log.debug("flush -> q=%s", key)
    queues.flush(key, Some(clientDescription))
  }

  def delete(key: String) {
    checkBlockWrites("delete", key)
    log.debug("delete -> q=%s", key)
    queues.delete(key, Some(clientDescription))
  }

  def flushExpired(key: String) = {
    checkBlockWrites("flushExpired", key)
    log.debug("flush_expired -> q=%s", key)
    queues.flushExpired(key, clientDescription = Some(clientDescription))
  }

  private def withServerStatus[T](f: (ServerStatus) => T): T = {
    serverStatus match {
      case Some(s) => f(s)
      case None => throw new ServerStatusNotConfiguredException
    }
  }

  def safeCheckBlockReads: Boolean = serverStatus map { _.blockReads } getOrElse(false)

  def checkBlockReads(op: String, key: String) {
    if (safeCheckBlockReads) {
      log.debug("Blocking %s on '%s' (sid %d, %s)", op, key, sessionId, clientDescription)
      throw new AvailabilityException(op)
    }
  }

  def checkBlockWrites(op: String, key: String) {
    if (serverStatus map { _.blockWrites } getOrElse(false)) {
      log.debug("Blocking %s on '%s' (sid %d, %s)", op, key, sessionId, clientDescription)
      throw new AvailabilityException(op)
    }
  }

  def currentStatus: String = {
    log.debug("read status")
    withServerStatus(_.status.toString)
  }

  def setStatus(status: String) {
    log.debug("status to %s", status)
    withServerStatus(_.setStatus(status))
  }

  def markQuiescecent() {
    withServerStatus(_.markQuiescent)
  }

  def markReadOnly() {
    withServerStatus(_.markReadOnly)
  }

  def markUp() {
    withServerStatus(_.markUp)
  }

  def shutdown() {
    BackgroundProcess {
      Thread.sleep(100)
      ServiceTracker.shutdown()
    }
  }
}
