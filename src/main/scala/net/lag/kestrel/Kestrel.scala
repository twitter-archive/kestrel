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

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.conversions.time._
import com.twitter.finagle.{ClientConnection, Codec => FinagleCodec, Service => FinagleService}
import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.stats.OstrichStatsReceiver
import com.twitter.finagle.thrift._
import com.twitter.finagle.util.TimerFromNettyTimer
import com.twitter.logging.Logger
import com.twitter.naggati.Codec
import com.twitter.naggati.codec.{MemcacheResponse, MemcacheRequest, MemcacheCodec}
import com.twitter.ostrich.admin.{PeriodicBackgroundProcess, RuntimeEnvironment, Service, ServiceTracker}
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Duration, Eval, FuturePool, Future, Time, Timer}
import java.net.InetSocketAddress
import java.net.URI
import java.util.Collections._
import config._
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import org.apache.thrift.protocol.TBinaryProtocol
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
import org.jboss.netty.util.HashedWheelTimer
import java.io.File

class Kestrel(defaultQueueConfig: QueueConfig, builders: List[QueueBuilder], aliases: List[AliasBuilder],
              listenAddress: String, memcacheListenPort: Option[Int], textListenPort: Option[Int],
              thriftListenPort: Option[Int], queuePath: String,
              expirationTimerFrequency: Option[Duration], clientTimeout: Option[Duration],
              maxOpenTransactions: Int, connectionBacklog: Option[Int], statusFile: String,
              defaultStatus: Status, statusChangeGracePeriod: Duration, enableSessionTrace: Boolean,
              connectionLimitRefuseWrites: Option[Int], connectionLimitRefuseReads: Option[Int],
              zkConfig: Option[ZooKeeperConfig], beFactoryClass: Option[String])
      extends Service {
  private val log = Logger.get(getClass.getName)

  var queueCollection: QueueCollection = null
  var timer: Timer = null
  var journalSyncScheduler: ScheduledExecutorService = null
  var memcacheService: Option[Server] = None
  var textService: Option[Server] = None
  var thriftService: Option[Server] = None
  var expirationBackgroundProcess: Option[PeriodicBackgroundProcess] = None

  var serverStatus: ServerStatus = null

  def thriftCodec = ThriftServerFramedCodec()

  def traceSessions: Boolean = enableSessionTrace
  def checkConnectionLimits(currentCount: Int): (Boolean,Boolean) = {
    (connectionLimitRefuseWrites.map { v:Int => currentCount > v} getOrElse(false),
     connectionLimitRefuseReads.map { v:Int => currentCount > v} getOrElse(false))
  }

  private def finagledCodec[Req, Resp](codec: => Codec[Resp]) = {
    new FinagleCodec[Req, Resp] {
      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = Channels.pipeline(codec)
      }
    }
  }

  def startFinagleServer[Req, Resp](
    name: String,
    port: Int,
    finagleCodec: FinagleCodec[Req, Resp]
  )(factory: ClientConnection => FinagleService[Req, Resp]): Server = {
    val address = new InetSocketAddress(listenAddress, port)
    var builder = ServerBuilder()
      .codec(finagleCodec)
      .name(name)
      .keepAlive(true)
      .reportTo(new OstrichStatsReceiver)
      .bindTo(address)
    connectionBacklog.foreach { backlog => builder = builder.backlog(backlog) }
    clientTimeout.foreach { timeout => builder = builder.readTimeout(timeout) }
    // calling build() is equivalent to calling start() in finagle.
    val server = builder.build(factory)
    log.info("%s server started on %s", name, address)
    server
  }

  def startThriftServer(
    name: String,
    port: Int
  ): Server = {
    val address = new InetSocketAddress(listenAddress, port)
    var builder = ServerBuilder()
      .codec(thriftCodec)
      .name(name)
      .keepAlive(true)
      .reportTo(new OstrichStatsReceiver)
      .bindTo(address)
    connectionBacklog.foreach { backlog => builder = builder.backlog(backlog) }
    clientTimeout.foreach { timeout => builder = builder.readTimeout(timeout) }
    // calling build() is equivalent to calling start() in finagle.
    val server = builder.build(connection => {
      val handler = new ThriftHandler(connection, queueCollection, maxOpenTransactions, timer, Some(serverStatus))
      new ThriftFinagledService(handler, new TBinaryProtocol.Factory())
    })
    log.info("%s server started on %s", name, address)
    server
  }

  private def bytesRead(n: Int) {
    Stats.incr("bytes_read", n)
  }

  private def bytesWritten(n: Int) {
    Stats.incr("bytes_written", n)
  }

  def start() {
    log.info("Kestrel config: listenAddress=%s memcachePort=%s textPort=%s queuePath=%s " +
             "expirationTimerFrequency=%s clientTimeout=%s maxOpenTransactions=%d connectionBacklog=%s " +
             "statusFile=%s defaultStatus=%s statusChangeGracePeriod=%s enableSessionTrace=%s " +
             "connectionLimitRefuseWrites=%s connectionLimitRefuseReads=%s zookeeper=<%s> beFactoryClass=<%s>",
             listenAddress, memcacheListenPort, textListenPort, queuePath,
             expirationTimerFrequency, clientTimeout, maxOpenTransactions, connectionBacklog,
             statusFile, defaultStatus, statusChangeGracePeriod, enableSessionTrace,
             connectionLimitRefuseWrites, connectionLimitRefuseReads, zkConfig, beFactoryClass)

    Stats.setLabel("version", Kestrel.runtime.jarVersion)

    // this means no timeout will be at better granularity than 100 ms.
    val nettyTimer = new HashedWheelTimer(100, TimeUnit.MILLISECONDS)
    timer = new TimerFromNettyTimer(nettyTimer)

    journalSyncScheduler =
      new ScheduledThreadPoolExecutor(
        Runtime.getRuntime.availableProcessors,
        new NamedPoolThreadFactory("journal-sync", true),
        new RejectedExecutionHandler {
          override def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor) {
            log.warning("Rejected journal fsync")
          }
        })
    Journal.packer.start()

    try {
      val beFactory = beFactoryClass map { className =>
        StreamContainerFactory(className)
      } getOrElse new LocalFSContainerFactory(queuePath, statusFile, journalSyncScheduler)
      
      val streamContainer = beFactory.createStreamContainer()
      val statusStore = beFactory.createStatusStore()

      queueCollection = new QueueCollection(streamContainer, timer, defaultQueueConfig, builders, aliases)
      queueCollection.loadQueues()

      Stats.addGauge("items") { queueCollection.currentItems.toDouble }
      Stats.addGauge("bytes") { queueCollection.currentBytes.toDouble }
      Stats.addGauge("reserved_memory_ratio") { queueCollection.reservedMemoryRatio }

      serverStatus =
        zkConfig.map { cfg =>
          new ZooKeeperServerStatus(cfg, statusStore, timer, defaultStatus,
                                    statusChangeGracePeriod, None)
        } getOrElse {
          new ServerStatus(statusStore, timer, defaultStatus, statusChangeGracePeriod, None)
        }
      serverStatus.start()
    } catch {
      case e: InaccessibleQueuePath =>
        e.printStackTrace()
        throw e
    }

    serverStatus =
      zkConfig.map { cfg =>
        new ZooKeeperServerStatus(cfg, statusFile, timer, defaultStatus,
                                  statusChangeGracePeriod)
      } getOrElse {
        new ServerStatus(statusFile, timer, defaultStatus, statusChangeGracePeriod)
      }
    serverStatus.start()

    // finagle setup:
    val memcachePipelineFactoryCodec = finagledCodec[MemcacheRequest, MemcacheResponse] {
      MemcacheCodec.asciiCodec(bytesRead, bytesWritten)
    }
    memcacheService = memcacheListenPort.map { port =>
      startFinagleServer("kestrel-memcache", port, memcachePipelineFactoryCodec) { connection =>
        new MemcacheHandler(connection, queueCollection, maxOpenTransactions, Some(serverStatus))
      }
    }

    val textPipelineFactory = finagledCodec[TextRequest, TextResponse] {
      TextCodec(bytesRead, bytesWritten)
    }
    textService = textListenPort.map { port =>
      startFinagleServer("kestrel-text", port, textPipelineFactory) { connection =>
        new TextHandler(connection, queueCollection, maxOpenTransactions, Some(serverStatus))
      }
    }

    thriftService = thriftListenPort.map { port =>
      startThriftServer("kestrel-thrift", port)
    }

    // optionally, start a periodic timer to clean out expired items.
    expirationBackgroundProcess = expirationTimerFrequency.map { period =>
      log.info("Starting up background expiration task.")
      val taskDesc = Some(() => "<background expiration task>")
      val proc = new PeriodicBackgroundProcess("background-expiration", period) {
        def periodic() {
          Kestrel.this.queueCollection.flushAllExpired(taskDesc)

          // Now that we've cleaned out the queue, lets see if any of them are
          // ready to be expired.
          Kestrel.this.queueCollection.deleteExpiredQueues()
        }
      }
      proc.start()
      proc
    }

    // Order is important: the main endpoint published in zookeeper is the
    // first configured protocol in the list: memcache, thrift, text.
    serverStatus match {
      // register server sets iff it is zookeeper server status
      case zss: ZooKeeperServerStatus =>
        val endpoints =
          memcacheService.map { s => "memcache" -> s.localAddress } ++
          thriftService.map { s => "thrift" -> s.localAddress } ++
          textService.map { s => "text" -> s.localAddress }
        if (endpoints.nonEmpty) {
          val mainEndpoint = endpoints.head._1
          val inetEndpoints =
            endpoints.map { case (name, addr) => (name, addr.asInstanceOf[InetSocketAddress]) }
          zss.addEndpoints(mainEndpoint, inetEndpoints.toMap)
        } else {
          log.error("No protocols configured; set a listener port for at least one protocol.")
        }
      // do nothing for other type of server status
      case _ =>
    }
  }

  def shutdown() {
    log.info("Shutting down!")

    // finagle cannot drain connections if they have long wait operations
    // pending, so start a thread to periodically evict any waiters. Once
    // close is invoked on the endpoint, finagle will close their connections
    // at the completion of the evicted wait request.
    val kicker = new PeriodicBackgroundProcess("evict-waiters", 1.second) {
      def periodic() {
        queueCollection.evictWaiters()
      }
    }
    kicker.start()

    try {
      memcacheService.foreach { svc =>
        svc.close(30.seconds.fromNow)
        log.info("kestrel-memcache server stopped")
      }
      textService.foreach { svc =>
        svc.close(30.seconds.fromNow)
        log.info("kestrel-text server stopped")
      }
      thriftService.foreach { svc =>
        svc.close(30.seconds.fromNow)
        log.info("kestrel-thrift server stopped")
      }
    } finally {
      kicker.shutdown()
    }

    if (timer ne null) {
      timer.stop()
      timer = null
    }

    expirationBackgroundProcess.foreach { _.shutdown() }

    Journal.packer.shutdown()

    if (queueCollection ne null) {
      queueCollection.shutdown((serverStatus ne null) && serverStatus.gracefulShutdown)
      queueCollection = null
    }

    if (journalSyncScheduler ne null) {
      journalSyncScheduler.shutdown()
      journalSyncScheduler.awaitTermination(5, TimeUnit.SECONDS)
      journalSyncScheduler = null
    }

    log.info("Goodbye.")
  }

  override def reload() {
    try {
      log.info("Reloading %s ...", Kestrel.runtime.configFile)
      new Eval().apply[KestrelConfig](Kestrel.runtime.configFile).reload(this)
    } catch {
      case e: Eval.CompilerException =>
        log.error(e, "Error in config: %s", e)
        log.error(e.messages.flatten.mkString("\n"))
    }
  }

  def reload(newDefaultQueueConfig: QueueConfig, newQueueBuilders: List[QueueBuilder],
             newAliasBuilders: List[AliasBuilder]) {
    queueCollection.reload(newDefaultQueueConfig, newQueueBuilders, newAliasBuilders)
  }
}

object Kestrel {
  val log = Logger.get(getClass.getName)
  var kestrel: Kestrel = null
  var runtime: RuntimeEnvironment = null

  private val startTime = Time.now

  // track concurrent sessions
  val sessions = new AtomicInteger()
  val sessionIdGenerator = new AtomicInteger()

  def main(args: Array[String]): Unit = {
    try {
      runtime = RuntimeEnvironment(this, args)
      kestrel = runtime.loadRuntimeConfig[Kestrel]()

      Stats.addGauge("connections") { sessions.get().toDouble }

      kestrel.start()
    } catch {
      case e =>
        log.error(e, "Exception during startup; exiting!")

        // Shut down all registered services such as AdminHttpService properly
        // so that SBT does not refuse to shut itself down when 'sbt run -f ...'
        // fails.
        ServiceTracker.shutdown()

        System.exit(1)
    }
    log.info("Kestrel %s started.", runtime.jarVersion)
  }

  def uptime() = Time.now - startTime

  def traceSessions:Boolean = {
    if (kestrel != null) {
      kestrel.traceSessions
    }
    else {
      false
    }
  }

  def checkConnectionLimits(currentCount: Int): (Boolean,Boolean) = {
    if (kestrel != null) {
      kestrel.checkConnectionLimits(currentCount)
    }
    else {
      (false, false)
    }
  }
}
