package com.twitter.scarling

import java.net.InetSocketAddress
import java.nio.ByteOrder
import scala.actors.Actor
import scala.actors.Actor._
import scala.collection.mutable
import net.lag.ConfiggyExtensions._
import net.lag.configgy.Config
import net.lag.configgy.Configgy
import net.lag.logging.Logger
import org.apache.mina.common._
import org.apache.mina.transport.socket.nio.SocketSessionConfig
import com.twitter.scarling.memcache.ProtocolException


class ScarlingHandler(val session: IoSession, val config: Config) extends Actor {
    private val log = Logger.get

    private val IDLE_TIMEOUT = 60
    private val sessionID = ScarlingStats.sessionID.incr
    private val remoteAddress = session.getRemoteAddress.asInstanceOf[InetSocketAddress]


	if (session.getTransportType == TransportType.SOCKET) {
		session.getConfig.asInstanceOf[SocketSessionConfig].setReceiveBufferSize(2048)
	}

    // config can be null in unit tests
    val idleTimeout = if (config == null) IDLE_TIMEOUT else config.getInt("timeout", IDLE_TIMEOUT)
    if (idleTimeout > 0) {
        session.setIdleTime(IdleStatus.BOTH_IDLE, idleTimeout)
    }

    ScarlingStats.sessions.incr
    ScarlingStats.totalConnections.incr
    log.debug("New session %d from %s:%d", sessionID, remoteAddress.getHostName, remoteAddress.getPort)
    start

    def act = {
        loop {
            react {
                case MinaMessage.SessionOpened =>

                case MinaMessage.MessageReceived(msg) => handle(msg.asInstanceOf[memcache.Request])

                case MinaMessage.MessageSent(msg) =>

                case MinaMessage.ExceptionCaught(cause) => {
                    cause.getCause match {
                        case _: ProtocolException => writeResponse("CLIENT_ERROR\r\n")
                        case _ => {
                            log.error(cause, "Exception caught on session %d: %s", sessionID, cause.getMessage)
                            writeResponse("ERROR\r\n")
                        }
                    }
                    session.close
                }

                case MinaMessage.SessionClosed => {
                    log.debug("End of session %d", sessionID)
                    ScarlingStats.sessions.decr
                    exit()
                }

                case MinaMessage.SessionIdle(status) => {
                    log.debug("Idle timeout on session %s", session)
                    session.close
                }
            }
        }
    }

    private def writeResponse(out: String) = {
        val bytes = out.getBytes
        session.write(new memcache.Response(ByteBuffer.wrap(bytes)))
    }

    private def writeResponse(out: String, data: Array[Byte]) = {
        val bytes = out.getBytes
        val buffer = ByteBuffer.allocate(bytes.length + data.length + 7)
        buffer.put(bytes)
        buffer.put(data)
        buffer.put("\r\nEND\r\n".getBytes)
        buffer.flip
        ScarlingStats.bytesWritten.incr(buffer.capacity)
        session.write(new memcache.Response(buffer))
    }

    private def handle(request: memcache.Request) = {
        request.line(0) match {
            case "GET" => get(request.line(1))
            case "SET" => set(request.line(1), request.line(2).toInt, request.line(3).toInt, request.data.get)
            case "STATS" => stats
            case "SHUTDOWN" => shutdown
        }
    }

    private def get(name: String): Unit = {
        ScarlingStats.getRequests.incr
        Scarling.queues.remove(name) match {
            case None => writeResponse("END\r\n")
            case Some(data) => writeResponse("VALUE " + name + " 0 " + data.length + "\r\n", data)
        }
    }

    private def set(name: String, flags: Int, expiry: Int, data: Array[Byte]) = {
        ScarlingStats.setRequests.incr
        if (Scarling.queues.add(name, data, expiry)) {
            writeResponse("STORED\r\n")
        } else {
            writeResponse("NOT_STORED\r\n")
        }
    }

    private def stats = {
        var report = new mutable.ArrayBuffer[(String, String)]
        report += (("uptime", Scarling.uptime.toString))
        report += (("time", (System.currentTimeMillis / 1000).toString))
        report += (("version", Scarling.VERSION))
        report += (("curr_items", Scarling.queues.currentItems.toString))
        report += (("total_items", Scarling.queues.totalAdded.toString))
        report += (("bytes", Scarling.queues.currentBytes.toString))
        report += (("curr_connections", ScarlingStats.sessions.toString))
        report += (("total_connections", ScarlingStats.totalConnections.toString))
        report += (("cmd_get", ScarlingStats.getRequests.toString))
        report += (("cmd_set", ScarlingStats.setRequests.toString))
        report += (("get_hits", Scarling.queues.queueHits.toString))
        report += (("get_misses", Scarling.queues.queueMisses.toString))
        report += (("bytes_read", ScarlingStats.bytesRead.toString))
        report += (("bytes_written", ScarlingStats.bytesWritten.toString))
        report += (("limit_maxbytes", "0"))                         // ???

        for (qName <- Scarling.queues.queueNames) {
            val (size, bytes, totalItems, journalSize, totalExpired, currentAge) = Scarling.queues.stats(qName)
            report += (("queue_" + qName + "_items", size.toString))
            report += (("queue_" + qName + "_total_items", totalItems.toString))
            report += (("queue_" + qName + "_logsize", journalSize.toString))
            report += (("queue_" + qName + "_expired_items", totalExpired.toString))
            report += (("queue_" + qName + "_age", currentAge.toString))
        }

        val summary = (for (item <- report) yield "STAT %s %s".format(item._1, item._2)).mkString("", "\r\n", "\r\nEND\r\n")
        writeResponse(summary)
    }

    private def shutdown = {
        Scarling.shutdown
    }

}
