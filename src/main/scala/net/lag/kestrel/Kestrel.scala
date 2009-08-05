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
import scala.actors.{Actor, Scheduler}
import scala.actors.Actor._
import scala.collection.mutable
import org.apache.mina.core.session.IoSession
import org.apache.mina.filter.codec.ProtocolCodecFilter
import org.apache.mina.transport.socket.SocketAcceptor
import org.apache.mina.transport.socket.nio.{NioProcessor, NioSocketAcceptor}
import _root_.net.lag.configgy.{Config, ConfigMap, Configgy, RuntimeEnvironment}
import _root_.net.lag.logging.Logger
import _root_.net.lag.naggati.IoHandlerActorAdapter


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
  private val log = Logger.get
  val runtime = new RuntimeEnvironment(getClass)

  var queues: QueueCollection = null

  private val _expiryStats = new mutable.HashMap[String, Int]
  private val _startTime = Time.now

  var acceptorExecutor: ExecutorService = null
  var acceptor: SocketAcceptor = null

  private val deathSwitch = new CountDownLatch(1)

  val DEFAULT_PORT = 22133


  def main(args: Array[String]): Unit = {
    runtime.load(args)
    startup(Configgy.config)
  }

  def configure(config: ConfigMap): Unit = {
    // fill in defaults for all queues
    PersistentQueue.maxItems = config.getInt("max_items", Math.MAX_INT)
    PersistentQueue.maxSize = config.getLong("max_size", Math.MAX_LONG)
    PersistentQueue.maxItemSize = config.getLong("max_item_size", Math.MAX_LONG)
    PersistentQueue.maxAge = config.getInt("max_age", 0)
    PersistentQueue.maxJournalSize = config.getInt("max_journal_size", 16 * 1024 * 1024)
    PersistentQueue.maxMemorySize = config.getInt("max_memory_size", 128 * 1024 * 1024)
    PersistentQueue.maxJournalOverflow = config.getInt("max_journal_overflow", 10)
    PersistentQueue.discardOldWhenFull = config.getBool("discard_old_when_full", false)
    PersistentQueue.keepJournal = config.getBool("journal", true)
    PersistentQueue.syncJournal = config.getBool("sync_journal", false)
  }

  def startup(config: Config): Unit = {
    // this one is used by the actor initialization, so can only be set at startup.
    val maxThreads = config.getInt("max_threads", Runtime.getRuntime().availableProcessors * 2)
    System.setProperty("actors.maxPoolSize", maxThreads.toString)
    log.debug("max_threads=%d", maxThreads)

    val listenAddress = config.getString("host", "0.0.0.0")
    val listenPort = config.getInt("port", DEFAULT_PORT)
    queues = new QueueCollection(config.getString("queue_path", "/tmp"), config.configMap("queues"))
    configure(config)
    config.subscribe { c => configure(c.getOrElse(new Config)) }

    queues.loadQueues()

    acceptorExecutor = Executors.newCachedThreadPool()
    acceptor = new NioSocketAcceptor(acceptorExecutor, new NioProcessor(acceptorExecutor))

    // mina setup:
    acceptor.setBacklog(1000)
    acceptor.setReuseAddress(true)
    acceptor.getSessionConfig.setTcpNoDelay(true)
    acceptor.getFilterChain.addLast("codec", new ProtocolCodecFilter(memcache.Codec.encoder,
      memcache.Codec.decoder))
    acceptor.setHandler(new IoHandlerActorAdapter(session => new KestrelHandler(session, config)))
    acceptor.bind(new InetSocketAddress(listenAddress, listenPort))

    // expose config thru JMX.
    config.registerWithJmx("net.lag.kestrel")

    log.info("Kestrel started.")

    // make sure there's always one actor running so scala 2.7.2 doesn't kill off the actors library.
    actor {
      deathSwitch.await
    }
  }

  def shutdown(): Unit = {
    log.info("Shutting down!")
    queues.shutdown
    acceptor.unbind
    acceptor.dispose
    Scheduler.shutdown
    acceptorExecutor.shutdown
    // the line below causes a 1 second pause in unit tests. :(
    //acceptorExecutor.awaitTermination(5, TimeUnit.SECONDS)
    deathSwitch.countDown
  }

  def uptime() = (Time.now - _startTime) / 1000
}
