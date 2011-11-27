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
import java.util.concurrent.{Executors, ExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.{immutable, mutable}
import com.twitter.conversions.time._
import com.twitter.finagle.builder.Server
import com.twitter.libkestrel.config._
import com.twitter.logging.Logger
import com.twitter.ostrich.admin.{PeriodicBackgroundProcess, RuntimeEnvironment, Service, ServiceTracker}
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Duration, Eval, Time, Timer => TTimer, TimerTask => TTimerTask}
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.{Channel, ChannelFactory, ChannelPipelineFactory, Channels}
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.util.{HashedWheelTimer, Timeout, Timer, TimerTask}
import org.apache.thrift.protocol._
import com.twitter.finagle.thrift._
import config._

import com.twitter.finagle.builder.{ServerBuilder, Server => FinagleServer}
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.util.{Timer => FinagleTimer}
import com.twitter.libkestrel._
import com.twitter.util.Future
import com.twitter.naggati.Codec
import com.twitter.naggati.codec.{MemcacheResponse, MemcacheRequest, MemcacheCodec}
import com.twitter.finagle.{ClientConnection, Codec => FinagleCodec, Service => FinagleService}

class Kestrel(defaultQueueBuilder: QueueBuilder, queueBuilders: Seq[QueueBuilder],
              listenAddress: String, memcacheListenPort: Option[Int], textListenPort: Option[Int],
              thriftListenPort: Option[Int], queuePath: String,
              expirationTimerFrequency: Option[Duration], clientTimeout: Option[Duration],
              maxOpenTransactions: Int)
      extends Service {
  private val log = Logger.get(getClass.getName)

  var queueCollection: QueueCollection = null
  var timer: Timer = null
  var memcacheService: Option[FinagleServer] = None
  var textService: Option[FinagleServer] = None
  var textAcceptor: Option[Channel] = None
  var thriftService: Option[FinagleServer] = None

  def thriftCodec = ThriftServerFramedCodec()

  private def finagledCodec[Req, Resp](codec: => Codec[Resp]) = {
    new FinagleCodec[Req, Resp] {
      def pipelineFactory = codec.pipelineFactory
    }
  }

  def startFinagleServer[Req, Resp](
    name: String,
    port: Int,
    finagleCodec: FinagleCodec[Req, Resp]
  )(factory: ClientConnection => FinagleService[Req, Resp]): FinagleServer = {
    val address = new InetSocketAddress(listenAddress, port)
    val builder = ServerBuilder()
      .codec(finagleCodec)
      .name(name)
      .reportTo(new OstrichStatsReceiver)
      .bindTo(address)
    clientTimeout.foreach { timeout => builder.readTimeout(timeout) }
    // calling build() is equivalent to calling start() in finagle.
    builder.build(factory)
  }

  def startThriftServer(
    name: String,
    port: Int
  ): FinagleServer = {
    val address = new InetSocketAddress(listenAddress, port)
    val builder = ServerBuilder()
      .codec(thriftCodec)
      .name(name)
      .reportTo(new OstrichStatsReceiver)
      .bindTo(address)
    clientTimeout.foreach { timeout => builder.readTimeout(timeout) }
    // calling build() is equivalent to calling start() in finagle.
    builder.build(connection => {
      val handler = new ThriftHandler(connection, queueCollection, maxOpenTransactions)
      new ThriftFinagledService(handler, new TBinaryProtocol.Factory())
    })
  }

  private def bytesRead(n: Int) {
    Stats.incr("bytes_read", n)
  }

  private def bytesWritten(n: Int) {
    Stats.incr("bytes_written", n)
  }

  def start() {
    log.info("Kestrel config: listenAddress=%s memcachePort=%s textPort=%s queuePath=%s " +
             "expirationTimerFrequency=%s clientTimeout=%s maxOpenTransactions=%d",
             listenAddress, memcacheListenPort, textListenPort, queuePath,
             expirationTimerFrequency, clientTimeout, maxOpenTransactions)

    // this means no timeout will be at better granularity than 10ms.
    // FIXME: would make more sense to use the finagle Timer. but they'd have to expose it.
    timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
    queueCollection = new QueueCollection(queuePath, new FinagleTimer(timer), defaultQueueBuilder,
      queueBuilders)
    queueCollection.loadQueues()

    Stats.addGauge("items") { queueCollection.currentItems.toDouble }
    Stats.addGauge("bytes") { queueCollection.currentBytes.toDouble }

    // finagle setup:
    val memcachePipelineFactoryCodec = finagledCodec[MemcacheRequest, MemcacheResponse] {
      MemcacheCodec.asciiCodec(bytesRead, bytesWritten)
    }
    memcacheService = memcacheListenPort.map { port =>
      startFinagleServer("kestrel-memcache", port, memcachePipelineFactoryCodec) { connection =>
        new MemcacheHandler(connection, queueCollection, maxOpenTransactions)
      }
    }

    val textPipelineFactory = finagledCodec[TextRequest, TextResponse] {
      TextCodec(bytesRead, bytesWritten)
    }
    textService = textListenPort.map { port =>
      startFinagleServer("kestrel-text", port, textPipelineFactory) { connection =>
        new TextHandler(connection, queueCollection, maxOpenTransactions)
      }
    }

    thriftService = thriftListenPort.map { port => startThriftServer("kestrel-thrift", port) }

    // optionally, start a periodic timer to clean out expired items.
    if (expirationTimerFrequency.isDefined) {
      log.info("Starting up background expiration task.")
      new PeriodicBackgroundProcess("background-expiration", expirationTimerFrequency.get) {
        def periodic() {
          Kestrel.this.queueCollection.flushAllExpired()
        }
      }.start()
    }
  }

  def shutdown() {
    log.info("Shutting down!")

    memcacheService.foreach { _.close() }
    textService.foreach { _.close() }
    thriftService.foreach { _.close() }
    queueCollection.shutdown()

    timer.stop()
    timer = null
    log.info("Goodbye.")
  }
}

object Kestrel {
  val log = Logger.get(getClass.getName)
  var kestrel: Kestrel = null
  var runtime: RuntimeEnvironment = null

  private val startTime = Time.now

  // track concurrent sessions
  val sessions = new AtomicInteger()
  val sessionId = new AtomicInteger()

  def main(args: Array[String]): Unit = {
    try {
      runtime = RuntimeEnvironment(this, args)
      kestrel = runtime.loadRuntimeConfig[Kestrel]()

      Stats.addGauge("connections") { sessions.get().toDouble }

      kestrel.start()
    } catch {
      case e =>
        log.error(e, "Exception during startup; exiting!")
        System.exit(1)
    }
    log.info("Kestrel %s started.", runtime.jarVersion)
  }

  def uptime() = Time.now - startTime
}
