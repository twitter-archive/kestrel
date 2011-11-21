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
import com.twitter.finagle.{ClientConnection}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
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

class ThriftHandler (
  connection: ClientConnection,
  queueCollection: QueueCollection,
  maxOpenReads: Int
) extends thrift.Kestrel.FutureIface {
  val log = Logger.get(getClass.getName)

  val sessionId = Kestrel.sessionId.incrementAndGet()
  val handler = new KestrelHandler(queueCollection, maxOpenReads, clientDescription, sessionId)
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

  def get(queueName: String, maxItems: Int, timeoutMsec: Int, autoConfirm: Boolean): Future[Seq[thrift.Item]] = {
    val expiry = if (timeoutMsec == 0) None else Some(timeoutMsec.milliseconds.fromNow)
    val future = new Promise[Seq[thrift.Item]]()
    val rv = new mutable.ListBuffer[thrift.Item]()
    handler.monitorUntil(queueName, expiry, maxItems, !autoConfirm) { itemOption =>
      itemOption match {
        case None => {
          future.setValue(rv.toList)
        }
        case Some(item) => {
          rv += new thrift.Item(ByteBuffer.wrap(item.data), item.xid)
        }
      }
    }
    future
  }

  def confirm(queueName: String, xids: Set[Long]): Future[Int] = {
    Future(handler.closeReads(queueName, xids.map { _.toInt }))
  }

  def abort(queueName: String, xids: Set[Long]): Future[Int] = {
    Future(handler.abortReads(queueName, xids.map { _.toInt }))
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
