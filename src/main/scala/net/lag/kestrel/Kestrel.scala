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
import java.util.concurrent.{CountDownLatch, Executors, ExecutorService, TimeUnit}
import scala.collection.{immutable, mutable}
import com.twitter.actors.{Actor, Scheduler}
import com.twitter.actors.Actor._
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.naggati.{ActorHandler, NettyMessage}
import com.twitter.naggati.codec.MemcacheCodec
import com.twitter.ostrich.{RuntimeEnvironment, Service, ServiceTracker}
import com.twitter.util.{Duration, Eval, Time}
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.{Channel, ChannelFactory, ChannelPipelineFactory, Channels}
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.util.{HashedWheelTimer, Timeout, Timer, TimerTask}
import config._

object KestrelStats {
  val bytesRead = new Counter
  val bytesWritten = new Counter
  val sessions = new Counter
  val totalConnections = new Counter
  val getRequests = new Counter
  val setRequests = new Counter
  val peekRequests = new Counter
  val sessionID = new Counter
}

class Kestrel(defaultQueueConfig: QueueConfig, builders: List[QueueBuilder], maxThreads: Int,
              listenAddress: String, listenPort: Int, queuePath: String, protocol: config.Protocol,
              expirationTimerFrequency: Duration, clientTimeout: Duration, maxOpenTransactions: Int)
      extends Service {
  private val log = Logger.get(getClass.getName)

  var queueCollection: QueueCollection = null
  var timer: Timer = null
  var executor: ExecutorService = null
  var channelFactory: ChannelFactory = null
  var memcacheAcceptor: Option[Channel] = None
  var textAcceptor: Option[Channel] = None
  val channelGroup = new DefaultChannelGroup("channels")

  private val deathSwitch = new CountDownLatch(1)

  def start(runtime: RuntimeEnvironment) {
    log.info("Kestrel config: maxThreads=%d listenAddress=%s port=%d queuePath=%s protocol=%s " +
             "expirationTimerFrequency=%s clientTimeout=%s maxOpenTransactions=%d",
             maxThreads, listenAddress, listenPort, queuePath, protocol, expirationTimerFrequency,
             clientTimeout, maxOpenTransactions)

    System.setProperty("actors.maxPoolSize", maxThreads.toString)

    queueCollection = new QueueCollection(queuePath, defaultQueueConfig, builders)
    queueCollection.loadQueues()
    // FIXME: reload?

    // netty setup:
    timer = new HashedWheelTimer()
    executor = Executors.newCachedThreadPool()
    channelFactory = new NioServerSocketChannelFactory(executor, executor)
    val filter: NettyMessage.Filter = immutable.Set(
      classOf[NettyMessage.MessageReceived],
      classOf[NettyMessage.ExceptionCaught],
      classOf[NettyMessage.ChannelIdle],
      classOf[NettyMessage.ChannelDisconnected])

    val memcachePipelineFactory = new ChannelPipelineFactory() {
      def getPipeline() = {
        val protocolCodec = protocol match {
          case Protocol.Ascii => MemcacheCodec.asciiCodec
          case Protocol.Binary => throw new Exception("Binary protocol not supported yet.")
        }
        val actorHandler = new ActorHandler(filter, { channel =>
          new MemcacheHandler(channel, channelGroup, queueCollection, maxOpenTransactions, clientTimeout)
        })
        Channels.pipeline(protocolCodec, actorHandler)
      }
    }
    memcacheAcceptor = Some(makeAcceptor(channelFactory, memcachePipelineFactory,
                                         new InetSocketAddress(listenAddress, listenPort)))

    val textPipelineFactory = new ChannelPipelineFactory() {
      def getPipeline() = {
        val protocolCodec = TextCodec.decoder
        val actorHandler = new ActorHandler(filter, { channel =>
          new TextHandler(channel, channelGroup, queueCollection, maxOpenTransactions, clientTimeout)
        })
        Channels.pipeline(protocolCodec, actorHandler)
      }
    }
    textAcceptor = Some(makeAcceptor(channelFactory, textPipelineFactory,
                                     new InetSocketAddress(listenAddress, 2222)))

    // optionally, start a periodic timer to clean out expired items.
    if (expirationTimerFrequency > 0.milliseconds) {
      log.info("Starting up background expiration task.")
      val expirationTask = new TimerTask {
        def run(timeout: Timeout) {
          val expired = Kestrel.this.queueCollection.flushAllExpired()
          if (expired > 0) {
            log.info("Expired %d item(s) from queues automatically.", expired)
          }
          timer.newTimeout(this, expirationTimerFrequency.inMilliseconds, TimeUnit.MILLISECONDS)
        }
      }
      expirationTask.run(null)
    }

    actor {
      deathSwitch.await()
    }

    log.info("Kestrel %s started.", runtime.jarVersion)
  }

  def shutdown() {
    log.info("Shutting down!")
    deathSwitch.countDown()

    memcacheAcceptor.foreach { _.close().awaitUninterruptibly() }
    textAcceptor.foreach { _.close().awaitUninterruptibly() }
    queueCollection.shutdown()
    //Scheduler.shutdown
    channelGroup.close().awaitUninterruptibly()
    channelFactory.releaseExternalResources()

    executor.shutdown()
    executor.awaitTermination(5, TimeUnit.SECONDS)
    timer.stop()
    timer = null
    log.info("Goodbye.")
  }

  def quiesce() {
    shutdown()
  }

  override def reload() {
    // FIXME
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
  var kestrel: Kestrel = null
  var runtime: RuntimeEnvironment = null

  private val _expiryStats = new mutable.HashMap[String, Int]
  private val startTime = Time.now

  // voodoo?
  @volatile var scheduler = Scheduler.impl

  def main(args: Array[String]): Unit = {
    runtime = RuntimeEnvironment(this, args)
    try {
      Logger.configure(runtime.loggingConfigFile)
      kestrel = Eval[KestrelConfig](runtime.configFile)()
    } catch {
      case e: Eval.CompilerException =>
        Logger.get("").fatal(e, "Error in config: %s", e)
        Logger.get("").fatal(e.messages.flatten.mkString("\n"))
        System.exit(1)
    }
    ServiceTracker.register(kestrel)
    kestrel.start(runtime)

    // AdminServiceConfig.apply().apply(runtime) => blah.
  }

  def uptime() = Time.now - startTime
}
