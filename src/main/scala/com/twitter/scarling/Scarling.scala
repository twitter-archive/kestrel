package com.twitter.scarling

import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.{Executors, ExecutorService, TimeUnit}
import scala.actors.{Actor, Scheduler}
import scala.actors.Actor._
import scala.collection.mutable
import org.apache.mina.common._
import org.apache.mina.filter.codec.ProtocolCodecFilter
import org.apache.mina.transport.socket.nio.{SocketAcceptor, SocketAcceptorConfig, SocketSessionConfig}
import net.lag.configgy.{Config, ConfigMap, Configgy, RuntimeEnvironment}
import net.lag.logging.Logger


class Counter {
  private var value = 0

  def get = synchronized { value }
  def set(n: Int) = synchronized { value = n }
  def incr = synchronized { value += 1; value }
  def incr(n: Int) = synchronized { value += n; value }
  def decr = synchronized { value -= 1; value }
  override def toString = synchronized { value.toString }
}


object ScarlingStats {
  val bytesRead = new Counter
  val bytesWritten = new Counter
  val sessions = new Counter
  val totalConnections = new Counter
  val getRequests = new Counter
  val setRequests = new Counter
  val sessionID = new Counter
}


object Scarling {
  private val log = Logger.get
  val runtime = new RuntimeEnvironment(getClass)

  var queues: QueueCollection = null

  private val _expiryStats = new mutable.HashMap[String, Int]
  private val _startTime = System.currentTimeMillis

  ByteBuffer.setUseDirectBuffers(false)
  ByteBuffer.setAllocator(new SimpleByteBufferAllocator())

  var acceptorExecutor: ExecutorService = null
  var acceptor: IoAcceptor = null

  def main(args: Array[String]) = {
    runtime.load(args)
    startup(Configgy.config)
  }

  def configure(c: Option[ConfigMap]) = {
    for (config <- c) {
      PersistentQueue.maxJournalSize = config.getInt("max_journal_size", 16 * 1024 * 1024)
    }
  }

  def startup(config: Config) = {
    val listenAddress = config.getString("host", "0.0.0.0")
    val listenPort = config.getInt("port", 22122)
    queues = new QueueCollection(config.getString("queue_path", "/tmp"), config.configMap("queues"))
    configure(Some(config))
    config.subscribe(configure _)

    acceptorExecutor = Executors.newCachedThreadPool()
    acceptor = new SocketAcceptor(Runtime.getRuntime().availableProcessors() + 1, acceptorExecutor)

    // mina garbage:
    acceptor.getDefaultConfig.setThreadModel(ThreadModel.MANUAL)
    val saConfig = new SocketAcceptorConfig
    saConfig.setReuseAddress(true)
    saConfig.setBacklog(1000)
    saConfig.getSessionConfig.setTcpNoDelay(true)
    saConfig.getFilterChain.addLast("codec", new ProtocolCodecFilter(new memcache.Encoder, new memcache.Decoder))
    acceptor.bind(new InetSocketAddress(listenAddress, listenPort), new IoHandlerActorAdapter((session: IoSession) => new ScarlingHandler(session, config)), saConfig)

    log.info("Scarling started.")
  }

  def shutdown = {
    log.info("Shutting down!")
    queues.shutdown
    acceptor.unbindAll
    Scheduler.shutdown
    acceptorExecutor.shutdown
    acceptorExecutor.awaitTermination(5, TimeUnit.SECONDS)
  }

  def uptime = (System.currentTimeMillis - _startTime) / 1000
}
