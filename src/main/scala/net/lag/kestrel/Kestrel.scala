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
import com.twitter.naggati.{ActorHandler, NettyMessage}
import com.twitter.naggati.codec.MemcacheRequest
import com.twitter.xrayspecs.Time
import net.lag.configgy.{Config, ConfigMap, Configgy, RuntimeEnvironment}
import net.lag.logging.Logger
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.{Channel, ChannelFactory, ChannelPipelineFactory, Channels}
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import org.jboss.netty.util.{HashedWheelTimer, Timeout, Timer, TimerTask}

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

object Kestrel {
  private val log = Logger.get(getClass.getName)
  val runtime = new RuntimeEnvironment(getClass)

  var queues: QueueCollection = null

  private val _expiryStats = new mutable.HashMap[String, Int]
  private val _startTime = Time.now.inMilliseconds

  var executor: ExecutorService = null
  var channelFactory: ChannelFactory = null
  val channels = new DefaultChannelGroup("channels")
  var acceptor: Option[Channel] = None
  var timer: Timer = null

  private val deathSwitch = new CountDownLatch(1)

  val DEFAULT_PORT = 22133


  def main(args: Array[String]): Unit = {
    runtime.load(args)
    startup(Configgy.config)
  }

  def configure(config: ConfigMap): Unit = {
    // fill in defaults for all queues
    PersistentQueue.maxItems = config.getInt("max_items", Int.MaxValue)
    PersistentQueue.maxSize = config.getLong("max_size", Long.MaxValue)
    PersistentQueue.maxItemSize = config.getLong("max_item_size", Long.MaxValue)
    PersistentQueue.maxAge = config.getInt("max_age", 0)
    PersistentQueue.maxJournalSize = config.getInt("max_journal_size", 16 * 1024 * 1024)
    PersistentQueue.maxMemorySize = config.getInt("max_memory_size", 128 * 1024 * 1024)
    PersistentQueue.maxJournalOverflow = config.getInt("max_journal_overflow", 10)
    PersistentQueue.discardOldWhenFull = config.getBool("discard_old_when_full", false)
    PersistentQueue.keepJournal = config.getBool("journal", true)
    PersistentQueue.syncJournal = config.getBool("sync_journal", false)
  }

  def startup(config: Config): Unit = {
    log.info("Kestrel config: %s", config.toString)

    // this one is used by the actor initialization, so can only be set at startup.
    var maxThreads = config.getInt("max_threads", Runtime.getRuntime().availableProcessors * 2)

    /* If we don't set this to at least 4, we get an IllegalArgumentException when constructing
     * the ThreadPoolExecutor from inside FJTaskScheduler2 on a single-processor box.
     */
    if (maxThreads < 4) {
      maxThreads = 4
    }

    System.setProperty("actors.maxPoolSize", maxThreads.toString)

    val listenAddress = config.getString("host", "0.0.0.0")
    val listenPort = config.getInt("port", DEFAULT_PORT)
    queues = new QueueCollection(config.getString("queue_path", "/tmp"), config.configMap("queues"))
    configure(config)
    config.subscribe { c => configure(c.getOrElse(new Config)) }

    queues.loadQueues()

    // netty setup:
    timer = new HashedWheelTimer()
    executor = Executors.newCachedThreadPool()
    channelFactory = new NioServerSocketChannelFactory(executor, executor)
    val bootstrap = new ServerBootstrap(channelFactory)
    val pipeline = bootstrap.getPipeline()
    val protocolCodec = config.getString("protocol", "ascii") match {
      case "ascii" => MemcacheRequest.asciiDecoder
      case "binary" => throw new Exception("Binary protocol not supported yet.")
    }
    val filter: NettyMessage.Filter = immutable.Set(
      classOf[NettyMessage.MessageReceived],
      classOf[NettyMessage.ExceptionCaught],
      classOf[NettyMessage.ChannelIdle],
      classOf[NettyMessage.ChannelDisconnected])
    pipeline.addLast("codec", protocolCodec)
    pipeline.addLast("handler", new ActorHandler(filter, { channel =>
      new MemcacheHandler(channel, config)
    }))

    bootstrap.setOption("backlog", 1000)
    bootstrap.setOption("reuseAddress", true)
    bootstrap.setOption("child.keepAlive", true)
    bootstrap.setOption("child.tcpNoDelay", true)
    bootstrap.setOption("child.receiveBufferSize", 2048)
    acceptor = Some(bootstrap.bind(new InetSocketAddress(listenAddress, listenPort)))

    // expose config thru JMX.
    if (config.getBool("jmx", false)) {
      config.registerWithJmx("net.lag.kestrel")
    }

    // optionally, start a periodic timer to clean out expired items.
    val expirationTimerFrequency = config.getInt("expiration_timer_frequency_seconds", 0)
    if (expirationTimerFrequency > 0) {
      log.info("Starting up background expiration task.")
      val expirationTask = new TimerTask {
        def run(timeout: Timeout) {
          val expired = Kestrel.queues.flushAllExpired()
          if (expired > 0) {
            log.info("Expired %d item(s) from queues automatically.", expired)
          }
          timer.newTimeout(this, expirationTimerFrequency, TimeUnit.SECONDS)
        }
      }
      expirationTask.run(null)
    }

    log.info("Kestrel started.")
    actor {
      deathSwitch.await()
    }
  }

  def shutdown() {
    log.info("Shutting down!")
    deathSwitch.countDown()

    acceptor.foreach { _.close().awaitUninterruptibly() }
    queues.shutdown()
    Scheduler.shutdown
    channels.close().awaitUninterruptibly()
    channelFactory.releaseExternalResources()

    executor.shutdown()
    executor.awaitTermination(5, TimeUnit.SECONDS)
    timer.stop()
    timer = null
    log.info("Goodbye.")
  }

  def uptime() = (Time.now.inMilliseconds - _startTime) / 1000
}
