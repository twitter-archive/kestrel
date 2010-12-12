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

import java.io.IOException
import java.net.InetSocketAddress
import scala.collection.mutable
import com.twitter.actors.Actor
import com.twitter.actors.Actor._
import com.twitter.naggati.{NettyMessage, ProtocolError}
import com.twitter.naggati.codec.{MemcacheRequest, MemcacheResponse}
import com.twitter.{Duration, Time}
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.group.ChannelGroup
import org.jboss.netty.handler.timeout.IdleStateHandler

/**
 * Memcache protocol handler for a kestrel connection.
 */
class MemcacheHandler(val channel: Channel, val channelGroup: ChannelGroup,
                      queueCollection: QueueCollection, maxOpenTransactions: Int,
                      clientTimeout: Duration)
      extends KestrelHandler(queueCollection, maxOpenTransactions) with Actor {
  private val log = Logger.get

  private val remoteAddress = channel.getRemoteAddress.asInstanceOf[InetSocketAddress]

  if (clientTimeout > 0.milliseconds) {
    channel.getPipeline.addFirst("idle", new IdleStateHandler(Kestrel.kestrel.timer, 0, 0, clientTimeout.inSeconds.toInt))
  }

  channelGroup.add(channel)
  log.debug("New session %d from %s:%d", sessionID, remoteAddress.getHostName, remoteAddress.getPort)
  start

  protected def clientDescription: String = {
    "%s:%d".format(remoteAddress.getHostName, remoteAddress.getPort)
  }

  def act = {
    loop {
      react {
        case NettyMessage.MessageReceived(msg) =>
          handle(msg.asInstanceOf[MemcacheRequest])

        case NettyMessage.ExceptionCaught(cause) =>
          cause match {
            case _: ProtocolError =>
              new MemcacheResponse("CLIENT_ERROR").writeTo(channel)
            case _: IOException =>
              log.debug("I/O Exception on session %d: %s", sessionID, cause.toString)
            case _ =>
              log.error(cause, "Exception caught on session %d: %s", sessionID, cause.toString)
              new MemcacheResponse("ERROR").writeTo(channel)
          }
          channel.close()

        case NettyMessage.ChannelDisconnected() =>
          finish()
          exit()

        case NettyMessage.ChannelIdle(status) =>
          log.debug("Idle timeout on session %s", channel)
          channel.close()
      }
    }
  }

  private def handle(request: MemcacheRequest) = {
    KestrelStats.bytesRead.incr(request.bytesRead)
    request.line(0) match {
      case "get" =>
        get(request.line(1))
      case "monitor" =>
        monitor(request.line(1), request.line(2).toInt)
      case "confirm" =>
        if (closeTransactions(request.line(1), request.line(2).toInt)) {
          new MemcacheResponse("END").writeTo(channel)
        } else {
          new MemcacheResponse("ERROR").writeTo(channel)
        }
      case "set" =>
        try {
          if (setItem(request.line(1), request.line(2).toInt, request.line(3).toInt, request.data.get)) {
            new MemcacheResponse("STORED").writeTo(channel)
          } else {
            new MemcacheResponse("NOT_STORED").writeTo(channel)
          }
        } catch {
          case e: NumberFormatException =>
            throw new ProtocolError("bad request: " + request)
        }
      case "stats" =>
        stats()
      case "shutdown" =>
        shutdown()
      case "reload" =>
        Kestrel.kestrel.reload()
        new MemcacheResponse("Reloaded config.").writeTo(channel)
      case "flush" =>
        flush(request.line(1))
        new MemcacheResponse("END").writeTo(channel)
      case "flush_all" =>
        flushAllQueues()
        new MemcacheResponse("Flushed all queues.").writeTo(channel)
      case "dump_config" =>
        dumpConfig()
      case "dump_stats" =>
        dumpStats()
      case "delete" =>
        delete(request.line(1))
        new MemcacheResponse("END").writeTo(channel)
      case "flush_expired" =>
        new MemcacheResponse(flushExpired(request.line(1)).toString).writeTo(channel)
      case "flush_all_expired" =>
        val flushed = queues.flushAllExpired()
        new MemcacheResponse(flushed.toString).writeTo(channel)
      case "roll" =>
        rollJournal(request.line(1))
        new MemcacheResponse("END").writeTo(channel)
      case "version" =>
        version()
      case "quit" =>
        quit()
      case x =>
        new MemcacheResponse("CLIENT_ERROR").writeTo(channel)
    }
  }

  private def get(name: String): Unit = {
    var key = name
    var timeout = 0
    var closing = false
    var opening = false
    var aborting = false
    var peeking = false

    if (name contains '/') {
      val options = name.split("/")
      key = options(0)
      for (i <- 1 until options.length) {
        val opt = options(i)
        if (opt startsWith "t=") {
          timeout = opt.substring(2).toInt
        }
        if (opt == "close") closing = true
        if (opt == "open") opening = true
        if (opt == "abort") aborting = true
        if (opt == "peek") peeking = true
      }
    }

    if ((key.length == 0) || ((peeking || aborting) && (opening || closing)) || (peeking && aborting)) {
      new MemcacheResponse("CLIENT_ERROR").writeTo(channel)
      channel.close()
      return
    }

    if (aborting) {
      abortTransaction(key)
      new MemcacheResponse("END").writeTo(channel)
    } else {
      if (closing) {
        closeTransaction(key)
        if (!opening) new MemcacheResponse("END").writeTo(channel)
      }
      if (opening || !closing) {
        if (pendingTransactions.size(key) > 0 && !peeking && !opening) {
          log.warning("Attempt to perform a non-transactional fetch with an open transaction on " +
                      " '%s' (sid %d, %s)", key, sessionID, clientDescription)
          new MemcacheResponse("ERROR").writeTo(channel)
          channel.close()
          return
        }
        try {
          getItem(key, timeout, opening, peeking) {
            case None =>
              new MemcacheResponse("END").writeTo(channel)
            case Some(item) =>
              new MemcacheResponse("VALUE %s 0 %d".format(key, item.data.length), item.data).writeTo(channel)
          }
        } catch {
          case e: TooManyOpenTransactionsException =>
            log.warning("Attempt to open too many transactions on '%s' (sid %d, %s)", key, sessionID, clientDescription)
            new MemcacheResponse("ERROR").writeTo(channel)
            channel.close()
            return
        }
      }
    }
  }

  private def monitor(key: String, timeout: Int) {
    monitorUntil(key, Time.now + timeout.seconds) {
      case None =>
        new MemcacheResponse("END").writeTo(channel)
      case Some(item) =>
        new MemcacheResponse("VALUE %s 0 %d".format(key, item.data.length), item.data).writeTo(channel)
    }
  }

  private def stats() = {
    var report = new mutable.ArrayBuffer[(String, String)]
    report += (("uptime", Kestrel.uptime.toString))
    report += (("time", (Time.now.inMilliseconds / 1000).toString))
    report += (("version", Kestrel.runtime.jarVersion))
    report += (("curr_items", queues.currentItems.toString))
    report += (("total_items", queues.totalAdded.toString))
    report += (("bytes", queues.currentBytes.toString))
    report += (("curr_connections", KestrelStats.sessions.toString))
    report += (("total_connections", KestrelStats.totalConnections.toString))
    report += (("cmd_get", KestrelStats.getRequests.toString))
    report += (("cmd_set", KestrelStats.setRequests.toString))
    report += (("cmd_peek", KestrelStats.peekRequests.toString))
    report += (("get_hits", queues.queueHits.toString))
    report += (("get_misses", queues.queueMisses.toString))
    report += (("bytes_read", KestrelStats.bytesRead.toString))
    report += (("bytes_written", KestrelStats.bytesWritten.toString))

    for (qName <- queues.queueNames) {
      report ++= queues.stats(qName).map { case (k, v) => ("queue_" + qName + "_" + k, v) }
    }

    val summary = {
      for ((key, value) <- report) yield "STAT %s %s".format(key, value)
    }.mkString("", "\r\n", "\r\nEND")
    new MemcacheResponse(summary).writeTo(channel)
  }

  private def dumpConfig() = {
    val dump = new mutable.ListBuffer[String]
    for (qName <- queues.queueNames) {
      dump += "queue '" + qName + "' {"
      dump += queues.dumpConfig(qName).mkString("  ", "\r\n  ", "")
      dump += "}"
    }
    new MemcacheResponse(dump.mkString("", "\r\n", "\r\nEND\r\n")).writeTo(channel)
  }

  private def dumpStats() = {
    val dump = new mutable.ListBuffer[String]
    for (qName <- queues.queueNames) {
      dump += "queue '" + qName + "' {"
      dump += queues.stats(qName).map { case (k, v) => k + "=" + v }.mkString("  ", "\r\n  ", "")
      dump += "}"
    }
    new MemcacheResponse(dump.mkString("", "\r\n", "\r\nEND\r\n")).writeTo(channel)
  }

  private def version() = {
    new MemcacheResponse("VERSION " + Kestrel.runtime.jarVersion + "\r\n").writeTo(channel)
  }

  private def quit() = {
    channel.close()
  }
}
