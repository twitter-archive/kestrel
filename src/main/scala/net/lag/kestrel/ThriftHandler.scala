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
import com.twitter.finagle.ClientConnection
import com.twitter.logging.Logger
import com.twitter.util.{Duration, Future, Promise, Timer, TimerTask}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap => JConcurrentHashMap}
import org.apache.thrift.protocol.TProtocolFactory
import scala.collection.mutable
import scala.collection.Set

class ThriftFinagledService(val handler: ThriftHandler, override val protocolFactory: TProtocolFactory)
  extends thrift.Kestrel.FinagledService(handler, protocolFactory) {

  override def release() {
    handler.release()
    super.release()
  }
}

class ConcurrentHashMap[K, V] extends JConcurrentHashMap[K, V] {
  def getOrElseUpdate(key: K, op: => V): V = {
    Option(get(key)) match {
      case Some(value) => value
      case None =>
        // We may invoke op and discard the result if another
        // thread has set a value for key since our call to get
        val newValue = op
        val existingValue = putIfAbsent(key, newValue)
        if (existingValue != null) {
          existingValue // lost the race, discarding newValue
        } else {
          newValue // expect newValue now in the map
        }
    }
  }
}

case class QueueTransaction(val name: String, val xid: Int, var timerTask: Option[TimerTask])
object ThriftPendingReads {
  private val reads = new ConcurrentHashMap[Long, QueueTransaction]
  private val nextExternalXid = new AtomicLong(1L)
  private val sessionCounts = new ConcurrentHashMap[Int, ConcurrentHashMap[String, AtomicInteger]]()

  def setTimerTask(externalXid: Long, timerTask: TimerTask) {
    Option(reads.get(externalXid)) match {
      case Some(queueTransaction) => queueTransaction.timerTask = Some(timerTask)
      case None =>
    }
  }

  private[this] def counter(sessionId: Int, queue: String, createIfAbsent: Boolean): Option[AtomicInteger] = {
    val queueCountMap = if (createIfAbsent) {
      sessionCounts.getOrElseUpdate(sessionId, { new ConcurrentHashMap[String, AtomicInteger] })
    } else {
      val map = sessionCounts.get(sessionId)
      if (map eq null) return None
      map
    }

    Option(queueCountMap.get(queue)) match {
      case s@Some(_) => s
      case None if createIfAbsent => Some(queueCountMap.getOrElseUpdate(queue, { new AtomicInteger(0) }))
      case None => None
    }
  }

  def countPendingReads(sessionId: Int, queue: String): Int = {
    counter(sessionId, queue, false).map { _.get }.getOrElse(0)
  }

  def addPendingRead(sessionId: Int, queue: String, xid: Int): Option[Long] = {
    val externalXid = nextExternalXid.getAndIncrement
    reads.put(externalXid, QueueTransaction(queue, xid, None))
    counter(sessionId, queue, true).foreach { _.incrementAndGet() }
    Some(externalXid)
  }

  def closePendingReads(sessionId: Int, queue: String, externalXids: Set[Long])(f: (QueueTransaction) => Unit) = {
    var size = 0
    externalXids.foreach { externalXid =>
      Option(reads.remove(externalXid)).map { queueTrans =>
        queueTrans.timerTask.foreach { _.cancel() }
        f(queueTrans)
        counter(sessionId, queue, false).foreach { _.decrementAndGet() }
        size += 1
      }
    }
    size
  }

  def clearPendingReadCounts(sessionId: Int) { sessionCounts.remove(sessionId) }

  // testing only
  def reset() {
    nextExternalXid.set(1L)
  }
}

trait ThriftPendingReads {
  def queues: QueueCollection
  protected def log: Logger
  def sessionId: Int
  def clientDescription: () => String


  def closeReads(key: String, xids: Set[Long]): Int = {
    ThriftPendingReads.closePendingReads(sessionId, key, xids) { queueTrans =>
      queues.confirmRemove(queueTrans.name, queueTrans.xid)
    }
  }

  def abortReads(key: String, xids: Set[Long]): Int = {
    ThriftPendingReads.closePendingReads(sessionId, key, xids) { queueTrans =>
      queues.unremove(queueTrans.name, queueTrans.xid)
    }
  }

  def countPendingReads(key: String) = ThriftPendingReads.countPendingReads(sessionId, key)
  def addPendingRead(key: String, xid: Int) = { ThriftPendingReads.addPendingRead(sessionId, key, xid) }

  def cancelAllPendingReads() = {
    ThriftPendingReads.clearPendingReadCounts(sessionId)
    0 // we do not abort open reads on connection close
  }
}

class ThriftHandler (
  connection: ClientConnection,
  queueCollection: QueueCollection,
  maxOpenReads: Int,
  timer: Timer
) extends thrift.Kestrel.FutureIface {
  val log = Logger.get(getClass.getName)

  val sessionId = Kestrel.sessionId.incrementAndGet()
  val handler = new KestrelHandler(queueCollection, maxOpenReads, clientDescription _, sessionId) with ThriftPendingReads
  log.debug("New thrift session %d from %s", sessionId, clientDescription)

  protected def clientDescription: String = {
    val address = connection.remoteAddress.asInstanceOf[InetSocketAddress]
    "%s:%d".format(address.getHostName, address.getPort)
  }

  def put(queueName: String, items: Seq[ByteBuffer], expirationMsec: Int): Future[Int] = {
    var count = 0
    var expiry = if (expirationMsec == 0) None else Some(expirationMsec.milliseconds.fromNow)
    items.foreach { item =>
      val data = new Array[Byte](item.remaining)
      item.get(data)
      if (!handler.setItem(queueName, 0, expiry, data)) return Future(count)
      count += 1
    }
    Future(count)
  }

  def get(queueName: String, maxItems: Int, timeoutMsec: Int, autoAbortMsec: Int): Future[Seq[thrift.Item]] = {
    val expiry = if (timeoutMsec == 0) None else Some(timeoutMsec.milliseconds.fromNow)
    val future = new Promise[Seq[thrift.Item]]()
    val rv = new mutable.ListBuffer[thrift.Item]()
    handler.monitorUntil(queueName, expiry, maxItems, autoAbortMsec > 0) { (itemOption, externalXidOption) =>
      itemOption match {
        case None => {
          future.setValue(rv.toList)
        }
        case Some(item) => {
          val externalXid = externalXidOption.getOrElse(0L)
          rv += new thrift.Item(ByteBuffer.wrap(item.data), externalXid)
        }
      }
    }

    if (autoAbortMsec > 0) {
      val autoAbortTimeout = autoAbortMsec.milliseconds
      future onSuccess { items =>
        items.foreach { item =>
          val task = timer.schedule(autoAbortTimeout.fromNow) {
            handler.abortReads(queueName, Set(item.xid))
          }
          ThriftPendingReads.setTimerTask(item.xid, task)
        }
      }
    } else {
      future
    }
  }

  def confirm(queueName: String, xids: Set[Long]): Future[Int] = {
    Future(handler.closeReads(queueName, xids))
  }

  def abort(queueName: String, xids: Set[Long]): Future[Int] = {
    Future(handler.abortReads(queueName, xids))
  }

  def peek(queueName: String): Future[thrift.QueueInfo] = {
    handler.getItem(queueName, None, false, true).map { itemOption =>
      val data = itemOption.map { item => ByteBuffer.wrap(item.data) }
      val stats = queueCollection.stats(queueName).toMap
      new thrift.QueueInfo(data, stats("items").toLong, stats("bytes").toLong,
        stats("logsize").toLong, stats("age").toLong, stats("waiters").toInt,
        stats("open_transactions").toInt)
    }
  }

  def flushQueue(queueName: String): Future[Unit] = {
    handler.flush(queueName)
    Future.Unit
  }

  def flushAllQueues(): Future[Unit] = {
    handler.flushAllQueues()
    Future.Unit
  }

  def deleteQueue(queueName: String): Future[Unit] = {
    handler.delete(queueName)
    Future.Unit
  }

  def getVersion(): Future[String] = Future(Kestrel.runtime.jarVersion)

  def release() {
    log.debug("Ending session %d from %s", sessionId, clientDescription)
    handler.finish()
  }
}
