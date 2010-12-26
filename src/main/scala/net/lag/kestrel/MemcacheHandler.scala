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

import scala.collection.mutable
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.naggati.ProtocolError
import com.twitter.naggati.codec.{MemcacheRequest, MemcacheResponse}
import com.twitter.util.{Duration, Time}
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.group.ChannelGroup

/**
 * Memcache protocol handler for a kestrel connection.
 */
class MemcacheHandler(
  channel: Channel,
  channelGroup: ChannelGroup,
  queueCollection: QueueCollection,
  maxOpenTransactions: Int,
  clientTimeout: Duration)
extends NettyHandler[MemcacheRequest](channel, channelGroup, queueCollection, maxOpenTransactions, clientTimeout) {
  protected final def handle(request: MemcacheRequest) = {
    request.line(0) match {
      case "get" =>
        get(request.line(1))
      case "monitor" =>
        monitor(request.line(1), request.line(2).toInt)
      case "confirm" =>
        if (closeTransactions(request.line(1), request.line(2).toInt)) {
          channel.write(new MemcacheResponse("END"))
        } else {
          channel.write(new MemcacheResponse("ERROR"))
        }
      case "set" =>
        val now = Time.now
        val expiry = request.line(3).toInt
        val normalizedExpiry: Option[Time] = if (expiry == 0) {
          None
        } else if (expiry < 1000000) {
          Some(now + expiry.seconds)
        } else {
          Some(Time.epoch + expiry.seconds)
        }
        try {
          if (setItem(request.line(1), request.line(2).toInt, normalizedExpiry, request.data.get)) {
            channel.write(new MemcacheResponse("STORED"))
          } else {
            channel.write(new MemcacheResponse("NOT_STORED"))
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
        channel.write(new MemcacheResponse("Reloaded config."))
      case "flush" =>
        flush(request.line(1))
        channel.write(new MemcacheResponse("END"))
      case "flush_all" =>
        flushAllQueues()
        channel.write(new MemcacheResponse("Flushed all queues."))
      case "dump_config" =>
        dumpConfig()
      case "dump_stats" =>
        dumpStats()
      case "delete" =>
        delete(request.line(1))
        channel.write(new MemcacheResponse("END"))
      case "flush_expired" =>
        channel.write(new MemcacheResponse(flushExpired(request.line(1)).toString))
      case "flush_all_expired" =>
        val flushed = queues.flushAllExpired()
        channel.write(new MemcacheResponse(flushed.toString))
      case "roll" =>
        rollJournal(request.line(1))
        channel.write(new MemcacheResponse("END"))
      case "version" =>
        version()
      case "quit" =>
        quit()
      case x =>
        channel.write(new MemcacheResponse("CLIENT_ERROR"))
    }
  }

  protected final def handleProtocolError() {
    channel.write(new MemcacheResponse("CLIENT_ERROR"))
  }

  protected final def handleException(e: Throwable) {
    channel.write(new MemcacheResponse("ERROR"))
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
      channel.write(new MemcacheResponse("CLIENT_ERROR"))
      channel.close()
      return
    }

    if (aborting) {
      abortTransaction(key)
      channel.write(new MemcacheResponse("END"))
    } else {
      if (closing) {
        closeTransaction(key)
        if (!opening) channel.write(new MemcacheResponse("END"))
      }
      if (opening || !closing) {
        if (pendingTransactions.size(key) > 0 && !peeking && !opening) {
          log.warning("Attempt to perform a non-transactional fetch with an open transaction on " +
                      " '%s' (sid %d, %s)", key, sessionID, clientDescription)
          channel.write(new MemcacheResponse("ERROR"))
          channel.close()
          return
        }
        try {
          getItem(key, timeout, opening, peeking) {
            case None =>
              channel.write(new MemcacheResponse("END"))
            case Some(item) =>
              channel.write(new MemcacheResponse("VALUE %s 0 %d".format(key, item.data.length), item.data))
          }
        } catch {
          case e: TooManyOpenTransactionsException =>
            channel.write(new MemcacheResponse("ERROR"))
            channel.close()
            return
        }
      }
    }
  }

  private def monitor(key: String, timeout: Int) {
    monitorUntil(key, Time.now + timeout.seconds) {
      case None =>
        channel.write(new MemcacheResponse("END"))
      case Some(item) =>
        channel.write(new MemcacheResponse("VALUE %s 0 %d".format(key, item.data.length), item.data))
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
    channel.write(new MemcacheResponse(summary))
  }

  private def dumpConfig() = {
    val dump = new mutable.ListBuffer[String]
    for (qName <- queues.queueNames) {
      dump += "queue '" + qName + "' {"
      dump += queues.dumpConfig(qName).mkString("  ", "\r\n  ", "")
      dump += "}"
    }
    channel.write(new MemcacheResponse(dump.mkString("", "\r\n", "\r\nEND\r\n")))
  }

  private def dumpStats() = {
    val dump = new mutable.ListBuffer[String]
    for (qName <- queues.queueNames) {
      dump += "queue '" + qName + "' {"
      dump += queues.stats(qName).map { case (k, v) => k + "=" + v }.mkString("  ", "\r\n  ", "")
      dump += "}"
    }
    channel.write(new MemcacheResponse(dump.mkString("", "\r\n", "\r\nEND\r\n")))
  }

  private def version() = {
    channel.write(new MemcacheResponse("VERSION " + Kestrel.runtime.jarVersion + "\r\n"))
  }

  private def quit() = {
    channel.close()
  }
}
