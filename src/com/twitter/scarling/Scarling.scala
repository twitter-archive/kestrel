package com.twitter.scarling

import java.net.InetSocketAddress
import java.util.concurrent.Executors
import scala.actors.{Actor, Scheduler}
import scala.actors.Actor._
import scala.collection.mutable
import org.apache.mina.common._
import org.apache.mina.filter.codec.ProtocolCodecFilter
import org.apache.mina.transport.socket.nio.{SocketAcceptor, SocketAcceptorConfig, SocketSessionConfig}
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
    
    private var listenAddress = "0.0.0.0"
    private var listenPort = 22122
    
    private[scarling] val queues = new QueueCollection("/tmp")
    
    private val _expiryStats = new mutable.HashMap[String, Int]
    private val _startTime = System.currentTimeMillis
    
    val VERSION = "0.5"
    
    
    ByteBuffer.setUseDirectBuffers(false)
    ByteBuffer.setAllocator(new SimpleByteBufferAllocator())

    val acceptorExecutor = Executors.newCachedThreadPool()
    val acceptor: IoAcceptor = new SocketAcceptor(Runtime.getRuntime().availableProcessors() + 1, acceptorExecutor)

    def main(args: Array[String]) = {
        //Logger.get("").setLevel(Logger.TRACE)
        
        // mina garbage:
        acceptor.getDefaultConfig.setThreadModel(ThreadModel.MANUAL)
        val saConfig = new SocketAcceptorConfig
        saConfig.setReuseAddress(true)
        saConfig.getSessionConfig.setTcpNoDelay(true)
        saConfig.getFilterChain.addLast("codec", new ProtocolCodecFilter(new memcache.Encoder, new memcache.Decoder))
        acceptor.bind(new InetSocketAddress(listenAddress, listenPort), new IoHandlerActorAdapter((session: IoSession) => new ScarlingHandler(session)), saConfig)

        log.info("Scarling started.")
    }
    
    def shutdown = {
        log.info("Shutting down!")
        queues.shutdown
        acceptor.unbindAll
        Scheduler.shutdown
        acceptorExecutor.shutdown
    }

    def addExpiry(name: String): Unit = synchronized {
        if (! _expiryStats.contains(name)) {
            _expiryStats(name) = 1
        } else {
            _expiryStats(name) = _expiryStats(name) + 1
        }
    }
    
    def expiryStats(name: String) = synchronized {
        _expiryStats.get(name) match {
            case None => 0
            case Some(n) => n
        }
    }

    def uptime = (System.currentTimeMillis - _startTime) / 1000
}
