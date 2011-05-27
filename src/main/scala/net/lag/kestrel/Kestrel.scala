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
import com.twitter.naggati.Codec
import com.twitter.naggati.codec.{MemcacheResponse, MemcacheRequest, MemcacheCodec}
import com.twitter.finagle.{ClientConnection, ServerCodec, Service => FinagleService}

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
  var textService: Option[FinagleServer] = None
  var textAcceptor: Option[Channel] = None

  private def finagledCodec[Req, Resp](codec: => Codec[Resp]) = {
    new ServerCodec[Req, Resp] {
      val pipelineFactory = new ChannelPipelineFactory() {
        def getPipeline = Channels.pipeline(codec)
      }
    }
  }

  def startFinagleServer[Req, Resp](
    name: String,
    port: Int,
    serverCodec: ServerCodec[Req, Resp]
  )(factory: ClientConnection => FinagleService[Req, Resp]): FinagleServer = {
    val address = new InetSocketAddress(listenAddress, port)
    val builder = ServerBuilder()
      .codec(serverCodec)
      .name(name)
      .reportTo(new OstrichStatsReceiver)
      .bindTo(address)
    clientTimeout.foreach { timeout => builder.readTimeout(timeout) }
    // calling build() is equivalent to calling start() in fingale.
    builder.build(factory)
  }

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
    // FIXME: would make more sense to use the finagle Timer. but they'd have to expose it.
    timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
    queueCollection = new QueueCollection(queuePath, new FinagleTimer(timer), defaultQueueConfig, builders)
    queueCollection.loadQueues()

    // finagle setup:
    val memcachePipelineFactoryCodec = finagledCodec[MemcacheRequest, MemcacheResponse] {
      protocol match {
        case Protocol.Ascii => MemcacheCodec.asciiCodec(bytesRead, bytesWritten)
        case Protocol.Binary => throw new Exception("Binary protocol not supported yet.")
      }
    }
    memcacheService = memcacheListenPort.map { port =>
      startFinagleServer("kestrel-memcache", port, memcachePipelineFactoryCodec) { connection =>
        new MemcacheHandler(connection, queueCollection, maxOpenTransactions)
      }
    }

    val textPipelineFactory = finagledCodec[TextRequest, TextResponse] { TextCodec(bytesRead, bytesWritten) }
    textService = textListenPort.map { port =>
      startFinagleServer("kestrel-text", port, textPipelineFactory) { connection =>
        new TextHandler(connection, queueCollection, maxOpenTransactions)
      }
    }

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
    textService.foreach { _.close() }
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
