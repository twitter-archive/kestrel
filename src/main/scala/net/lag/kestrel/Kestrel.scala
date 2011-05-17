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
import com.twitter.finagle.{ServerCodec, Service => FinagleService}
import com.twitter.finagle.builder.Server
import com.twitter.logging.Logger
import com.twitter.ostrich.admin.{RuntimeEnvironment, Service, ServiceTracker}
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Duration, Eval, Time, Timer => TTimer, TimerTask => TTimerTask}
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.{Channel, ChannelFactory, ChannelPipelineFactory, Channels}
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.util.{HashedWheelTimer, Timeout, Timer, TimerTask}
import config._

import com.twitter.finagle.builder.{ServerBuilder, Server => FinagleServer}
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.util.{Timer => FinagleTimer}
import com.twitter.util.Future
import com.twitter.naggati.codec.{MemcacheResponse, MemcacheRequest, MemcacheCodec}

class Kestrel(defaultQueueConfig: QueueConfig, builders: List[QueueBuilder],
              listenAddress: String, memcacheListenPort: Option[Int], textListenPort: Option[Int],
              queuePath: String, protocol: config.Protocol,
              expirationTimerFrequency: Option[Duration], clientTimeout: Option[Duration],
              maxOpenTransactions: Int)
      extends Service {
  private val log = Logger.get(getClass.getName)

  var queueCollection: QueueCollection = null
  var timer: Timer = null
  var memcacheService: Option[FinagleServer] = None
  var textAcceptor: Option[Channel] = None

  private def bytesRead(n: Int) {
    Stats.incr("bytes_read", n)
  }

  private def bytesWritten(n: Int) {
    Stats.incr("bytes_written", n)
  }

  def start() {
    log.info("Kestrel config: listenAddress=%s memcachePort=%s textPort=%s queuePath=%s " +
             "protocol=%s expirationTimerFrequency=%s clientTimeout=%s maxOpenTransactions=%d",
             listenAddress, memcacheListenPort, textListenPort, queuePath, protocol,
             expirationTimerFrequency, clientTimeout, maxOpenTransactions)

    // this means no timeout will be at better granularity than 10ms.
    timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
    queueCollection = new QueueCollection(queuePath, new FinagleTimer(timer), defaultQueueConfig, builders)
    queueCollection.loadQueues()

    val memcachePipelineFactory = new ChannelPipelineFactory() {
      def getPipeline() = {
        val protocolCodec = protocol match {
          case Protocol.Ascii => MemcacheCodec.asciiCodec(bytesRead, bytesWritten)
          case Protocol.Binary => throw new Exception("Binary protocol not supported yet.")
        }
        Channels.pipeline(protocolCodec)
      }
    }
    val memcachePipelineFactoryCodec = new ServerCodec[MemcacheRequest, MemcacheResponse] {
      val pipelineFactory = memcachePipelineFactory
    }
    val memcacheHandler = new MemcacheHandler(queueCollection, maxOpenTransactions)
    val agricola = new FinagleService[MemcacheRequest, MemcacheResponse] {
      def apply(request: MemcacheRequest): Future[MemcacheResponse] = {
        memcacheHandler(request)
      }
    }
    // finagle setup:
    memcacheService = memcacheListenPort.map { port =>
      val address = new InetSocketAddress(listenAddress, port)
      val builder = ServerBuilder()
        .codec(memcachePipelineFactoryCodec)
        .name("kestrel-memcache")
        .reportTo(new OstrichStatsReceiver)
        .bindTo(address)
      clientTimeout.foreach { timeout => builder.readTimeout(timeout) }
      // calling build() is equivalent to calling start() in fingale.
      builder.build(agricola)
    }

    /*
    val textPipelineFactory = new ChannelPipelineFactory() {
      def getPipeline() = {
        val protocolCodec = TextCodec(bytesRead, bytesWritten)
        val handler = new TextHandler(channelGroup, queueCollection, maxOpenTransactions, clientTimeout)
        Channels.pipeline(protocolCodec, handler)
      }
    }
    textAcceptor = textListenPort.map { port =>
      val address = new InetSocketAddress(listenAddress, port)
      makeAcceptor(channelFactory, textPipelineFactory, address)
    }
*/

    // optionally, start a periodic timer to clean out expired items.
    if (expirationTimerFrequency.isDefined) {
      log.info("Starting up background expiration task.")
      val expirationTask = new TimerTask {
        def run(timeout: Timeout) {
          val expired = Kestrel.this.queueCollection.flushAllExpired()
          if (expired > 0) {
            log.info("Expired %d item(s) from queues automatically.", expired)
          }
          timer.newTimeout(this, expirationTimerFrequency.get.inMilliseconds, TimeUnit.MILLISECONDS)
        }
      }
      expirationTask.run(null)
    }
  }

  def shutdown() {
    log.info("Shutting down!")

    memcacheService.foreach { _.close() }
//    textAcceptor.foreach { _.close().awaitUninterruptibly() }
    queueCollection.shutdown()

    timer.stop()
    timer = null
    log.info("Goodbye.")
  }

  override def reload() {
    try {
      log.info("Reloading %s ...", Kestrel.runtime.configFile)
      Eval[KestrelConfig](Kestrel.runtime.configFile).reload(this)
    } catch {
      case e: Eval.CompilerException =>
        log.error(e, "Error in config: %s", e)
        log.error(e.messages.flatten.mkString("\n"))
    }
  }

  def reload(newDefaultQueueConfig: QueueConfig, newQueueBuilders: List[QueueBuilder]) {
    queueCollection.reload(newDefaultQueueConfig, newQueueBuilders)
  }

  private def makeAcceptor(channelFactory: ChannelFactory, pipelineFactory: ChannelPipelineFactory,
                           address: InetSocketAddress): Channel = {
    val bootstrap = new ServerBootstrap(channelFactory)
    bootstrap.setPipelineFactory(pipelineFactory)
    bootstrap.setOption("backlog", 1000)
    bootstrap.setOption("reuseAddress", true)
    bootstrap.setOption("child.keepAlive", true)
    bootstrap.setOption("child.tcpNoDelay", true)
    bootstrap.setOption("child.receiveBufferSize", 2048)
    bootstrap.bind(address)
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
    runtime = RuntimeEnvironment(this, args)
    kestrel = runtime.loadRuntimeConfig[Kestrel]()

    Stats.addGauge("connections") { sessions.get().toDouble }

    kestrel.start()
    log.info("Kestrel %s started.", runtime.jarVersion)
  }

  def uptime() = Time.now - startTime
}
