package com.twitter.scarling

import java.net.InetSocketAddress
import java.util.concurrent.Executors
import scala.actors.Actor
import scala.actors.Actor._
import scala.collection.mutable
import org.apache.mina.common._
import org.apache.mina.filter.codec.ProtocolCodecFilter
import org.apache.mina.transport.socket.nio.{SocketAcceptor, SocketAcceptorConfig, SocketSessionConfig}
import net.lag.logging.Logger


object Scarling {
    private val log = Logger.get
    
    private var listenAddress = "0.0.0.0"
    private var listenPort = 22122
    
    private[scarling] val queues = new QueueCollection("/tmp")
    
    private var _bytesRead = 0
    private var _bytesWritten = 0
    private val _expiryStats = new mutable.HashMap[String, Int]
    private val _startTime = System.currentTimeMillis
    
    val VERSION = "0.5"
    
    
    ByteBuffer.setUseDirectBuffers(false)
    ByteBuffer.setAllocator(new SimpleByteBufferAllocator())

    val acceptor: IoAcceptor = new SocketAcceptor(Runtime.getRuntime().availableProcessors() + 1, Executors.newCachedThreadPool())

    def main(args: Array[String]) = {
        //Logger.get("").setLevel(Logger.DEBUG)
        
        // mina garbage:
        acceptor.getDefaultConfig.setThreadModel(ThreadModel.MANUAL)
        val saConfig = new SocketAcceptorConfig
        saConfig.setReuseAddress(true)
        saConfig.getFilterChain.addLast("codec", new ProtocolCodecFilter(new memcache.ProtocolCodecFactory))
        acceptor.bind(new InetSocketAddress(listenAddress, listenPort), new IoHandlerActorAdapter((session: IoSession) => new ScarlingHandler(session)), saConfig)

        log.info("Scarling started.")
    }
    
    def shutdown = {
        log.info("Shutting down!")
        queues.shutdown
        acceptor.unbindAll
        System.exit(0)
    }

    def addBytesRead(n: Int) = synchronized {
        _bytesRead += n
    }
    
    def addBytesWritten(n: Int) = synchronized {
        _bytesWritten += n
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

    def bytesRead = synchronized { _bytesRead }
    def bytesWritten = synchronized { _bytesWritten }
    
    def uptime = (System.currentTimeMillis - _startTime) / 1000
}
