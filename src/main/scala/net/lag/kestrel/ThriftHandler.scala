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

import java.net.InetSocketAddress
import scala.collection.mutable
import scala.collection.Set
import com.twitter.util._
import config._
import net.lag.kestrel._
import net.lag.kestrel.config._
import java.nio.ByteBuffer
import com.twitter.logging.Logger
import java.util.concurrent.{TimeUnit}
import org.jboss.netty.util.{HashedWheelTimer, Timeout, Timer, TimerTask}
import com.twitter.finagle.{ClientConnection, Service}
import org.apache.thrift.protocol._
import com.twitter.finagle.thrift._
import net.lag.kestrel.thrift._

class ThriftFinagledService(val handler: ThriftHandler, override val protocolFactory: TProtocolFactory) 
    extends net.lag.kestrel.thrift.Kestrel.FinagledService(handler, protocolFactory) {
    
    override def release() {
        handler.release()
        super.release()
    }
}

class ThriftHandler (
  connection: ClientConnection,
  queueCollection: QueueCollection,
  maxOpenTransactions: Int
) extends net.lag.kestrel.thrift.Kestrel.FutureIface {

  val log = Logger.get(getClass.getName)

  val sessionId = Kestrel.sessionId.incrementAndGet()
  protected val handler = new KestrelHandler2(queueCollection, maxOpenTransactions, clientDescription, sessionId)
  log.debug("New session %d from %s", sessionId, clientDescription)

  println("New Session")

  def release() {
    println("Close Session")
    handler.finish()
  }

  protected def clientDescription: String = {
    val address = connection.remoteAddress.asInstanceOf[InetSocketAddress]
    "%s:%d".format(address.getHostName, address.getPort)
  }

  def get(key: String, reliable: Boolean = false): Future[Item] = {
    try {
      handler.getItem(key, None, reliable).map { itemOption =>
        itemOption match {
          case None => null
          case Some(item) => new Item(ByteBuffer.wrap(item.data), 
                                      if (reliable) Some(item.xid) else None)
        }
      }
    } catch {
      case e: TooManyOpenTransactionsException => 
        throw new KestrelException("Too many open transactions.")
    }
  }
  
  def multiget(key: String, maxItems: Int = 1, reliable: Boolean = false): Future[Seq[Item]] = {
    val futureList = for(i <- 1 to maxItems) 
      yield get(key, reliable)
    val agg = Future.collect(futureList.toSeq)
    agg.map(seq => seq.filter(_ != null))
  }
  
  def put(key: String, item: ByteBuffer): Future[Boolean] = {
    Future(handler.setItem(key, 0, None, item.array))
  }
  
  def multiput(key: String, items: Seq[ByteBuffer]): Future[Int] = {
    def putItemsUntilFirstFail(items: Seq[ByteBuffer], count: Int = 0): Int = {
      if(items.isEmpty) count
      else if(handler.setItem(key, 0, None, items.head.array)) 
        putItemsUntilFirstFail(items.tail, count + 1)
      else count
    }
    Future(putItemsUntilFirstFail(items))
  }
  
  def confirm(key: String, xids: Set[Int]): Future[Unit] = {
    for(xid <- xids) handler.confirmReliableRead(key, xid)
    Future(())
  }
  
  def abort(key: String, xids: Set[Int]): Future[Unit] = { // TODO: abort
    for(xid <- xids) handler.abortReliableRead(key, xid)
    Future(())
  }
  
  def flush(key: String): Future[Unit] = {
    handler.flush(key)
    Future(())
  }
}
