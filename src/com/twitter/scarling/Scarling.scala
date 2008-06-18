package com.twitter.scarling

import java.net.InetSocketAddress
import java.util.concurrent.{Executors, ExecutorService, TimeUnit}
import scala.actors.{Actor, Scheduler}
import scala.actors.Actor._
import scala.collection.mutable
import org.apache.mina.common._
import org.apache.mina.filter.codec.ProtocolCodecFilter
import org.apache.mina.transport.socket.nio.{SocketAcceptor, SocketAcceptorConfig, SocketSessionConfig}
import net.lag.ConfiggyExtensions._
import net.lag.configgy.Config
import net.lag.configgy.Configgy
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

    var queues: QueueCollection = null

    private val _expiryStats = new mutable.HashMap[String, Int]
    private val _startTime = System.currentTimeMillis

    private var configFilename = "/etc/scarling.conf"

    val VERSION = "0.5"


    ByteBuffer.setUseDirectBuffers(false)
    ByteBuffer.setAllocator(new SimpleByteBufferAllocator())

    var acceptorExecutor: ExecutorService = null
    var acceptor: IoAcceptor = null

    def main(args: Array[String]) = {
        parseArgs(args.toList)
        Configgy.configure(configFilename)
        startup(Configgy.config)
    }

    def startup(config: Config) = {
        val listenAddress = config.get("host", "0.0.0.0")
        val listenPort = config.getInt("port", 22122)
        queues = new QueueCollection(config.get("queue_path", "/tmp"))
        PersistentQueue.maxJournalSize = config.getInt("max_journal_size", 16 * 1024 * 1024)

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

    private def parseArgs(args: List[String]): Unit = {
        args match {
            case "-f" :: filename :: xs => {
                configFilename = filename
                parseArgs(xs)
            }
            case "--help" :: xs => {
                help
            }
            case unknown :: Nil => {
                Console.println("Unknown command-line option: " + unknown)
                help
            }
            case Nil =>
        }
    }

    private def help = {
        Console.println
        Console.println("scarling %s".format(VERSION))
        Console.println("options:")
        Console.println("    -f <filename>")
        Console.println("        load config file (default: %s)".format(configFilename))
        Console.println
        System.exit(0)
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
