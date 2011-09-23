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
import net.lag.kestrel.thrift.Item 

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
  protected val handler = new KestrelHandler(queueCollection, maxOpenTransactions, clientDescription, sessionId)
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

  def get(key: String, transaction: Boolean = false): Future[Item] = {
    Future(new Item(ByteBuffer.wrap("foo".getBytes), 0))
  }
  
  def multiget(key: String, maxItems: Int = 1, transaction: Boolean = false): Future[Seq[Item]] = {
    Future(List())
  }
  
  def put(key: String, item: ByteBuffer): Future[Unit] = {
    Future(())
  }
  
  def multiput(key: String, items: Seq[ByteBuffer]): Future[Unit] = {
    Future(())
  }
  
  def ack(key: String, xids: Set[Int]): Future[Unit] = {
    Future(())
  }
  
  def fail(key: String, xids: Set[Int]): Future[Unit] = {
    Future(())
  }
  
  def flush(key: String): Future[Unit] = {
    Future(())
  }
}
