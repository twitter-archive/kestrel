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
import com.twitter.naggati.codec.{MemcacheRequest, MemcacheResponse}
import com.twitter.ostrich.stats.Stats
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.group.ChannelGroup
import com.twitter.util.{Future, Duration, Time}
import com.twitter.finagle.ClientConnection
import java.net.InetSocketAddress
import com.twitter.naggati.{Codec, ProtocolError}

/**
 * Memcache protocol handler for a kestrel connection.
 */
class MemcacheHandler(connection: ClientConnection, queueCollection: QueueCollection, maxOpenTransactions: Int) {
  val log = Logger.get(getClass.getName)

  val sessionId = Kestrel.sessionId.incrementAndGet()
  protected val handler = new KestrelHandler(queueCollection, maxOpenTransactions, clientDescription, sessionId)

  protected def clientDescription: String = {
    val address = connection.remoteAddress.asInstanceOf[InetSocketAddress]
    "%s:%d".format(address.getHostName, address.getPort)
  }

  protected def disconnect() = {
    Future(new MemcacheResponse("") then Codec.Disconnect)
  }

  def finish() {
    handler.finish()
  }

  final def apply(request: MemcacheRequest): Future[MemcacheResponse] = {
    request.line(0) match {
      case "get" =>
        get(request.line(1))
      case "monitor" =>
        Future(monitor(request.line(1), request.line(2).toInt))
      case "confirm" =>
        if (handler.closeTransactions(request.line(1), request.line(2).toInt)) {
          Future(new MemcacheResponse("END"))
        } else {
          Future(new MemcacheResponse("ERROR"))
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
          if (handler.setItem(request.line(1), request.line(2).toInt, normalizedExpiry, request.data.get)) {
            Future(new MemcacheResponse("STORED"))
          } else {
            Future(new MemcacheResponse("NOT_STORED"))
          }
        } catch {
          case e: NumberFormatException =>
            Future(new MemcacheResponse("CLIENT_ERROR"))
        }
      case "stats" =>
        Future(stats())
      case "shutdown" =>
        handler.shutdown()
        disconnect()
      case "reload" =>
        Kestrel.kestrel.reload()
        Future(new MemcacheResponse("Reloaded config."))
      case "flush" =>
        handler.flush(request.line(1))
        Future(new MemcacheResponse("END"))
      case "flush_all" =>
        handler.flushAllQueues()
        Future(new MemcacheResponse("Flushed all queues."))
      case "dump_stats" =>
        Future(dumpStats())
      case "delete" =>
        handler.delete(request.line(1))
        Future(new MemcacheResponse("END"))
      case "flush_expired" =>
        Future(new MemcacheResponse(handler.flushExpired(request.line(1)).toString))
      case "flush_all_expired" =>
        val flushed = queueCollection.flushAllExpired()
        Future(new MemcacheResponse(flushed.toString))
      case "version" =>
        Future(version())
      case "quit" =>
        disconnect()
      case x =>
        Future(new MemcacheResponse("CLIENT_ERROR") then Codec.Disconnect)
    }
  }

  private def get(name: String): Future[MemcacheResponse] = {
    var key = name
    var timeout: Option[Time] = None
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
          timeout = Some(opt.substring(2).toInt.milliseconds.fromNow)
        }
        if (opt == "close") closing = true
        if (opt == "open") opening = true
        if (opt == "abort") aborting = true
        if (opt == "peek") peeking = true
      }
    }

    if ((key.length == 0) || ((peeking || aborting) && (opening || closing)) || (peeking && aborting)) {
      return Future(new MemcacheResponse("CLIENT_ERROR") then Codec.Disconnect)
    }

    if (aborting) {
      handler.abortTransaction(key)
      Future(new MemcacheResponse("END"))
    } else {
      if (closing) {
        handler.closeTransaction(key)
      }
      if (opening || !closing) {
        if (handler.pendingTransactions.size(key) > 0 && !peeking && !opening) {
          log.warning("Attempt to perform a non-transactional fetch with an open transaction on " +
                      " '%s' (sid %d, %s)", key, sessionId, clientDescription)
          return Future(new MemcacheResponse("ERROR") then Codec.Disconnect)
        }
        try {
          handler.getItem(key, timeout, opening, peeking).map { itemOption =>
            itemOption match {
              case None =>
                new MemcacheResponse("END")
              case Some(item) =>
                new MemcacheResponse("VALUE %s 0 %d".format(key, item.data.length), Some(item.data))
            }
          }
        } catch {
          case e: TooManyOpenTransactionsException =>
            Future(new MemcacheResponse("ERROR") then Codec.Disconnect)
        }
      } else {
        Future(new MemcacheResponse("END"))
      }
    }
  }

  private def monitor(key: String, timeout: Int): MemcacheResponse = {
    // FIXME
    handler.monitorUntil(key, Time.now + timeout.seconds) {
      case None =>
        new MemcacheResponse("END")
      case Some(item) =>
        new MemcacheResponse("VALUE %s 0 %d".format(key, item.data.length), Some(item.data))
    }
    null
  }

  private def stats() = {
    var report = new mutable.ArrayBuffer[(String, String)]
    report += (("uptime", Kestrel.uptime.inSeconds.toString))
    report += (("time", (Time.now.inMilliseconds / 1000).toString))
    report += (("version", Kestrel.runtime.jarVersion))
    report += (("curr_items", queueCollection.currentItems.toString))
    report += (("total_items", Stats.getCounter("total_items")().toString))
    report += (("bytes", queueCollection.currentBytes.toString))
    report += (("curr_connections", Kestrel.sessions.get().toString))
    report += (("total_connections", Stats.getCounter("total_connections")().toString))
    report += (("cmd_get", Stats.getCounter("cmd_get")().toString))
    report += (("cmd_set", Stats.getCounter("cmd_set")().toString))
    report += (("cmd_peek", Stats.getCounter("cmd_peek")().toString))
    report += (("get_hits", Stats.getCounter("get_hits")().toString))
    report += (("get_misses", Stats.getCounter("get_misses")().toString))
    report += (("bytes_read", Stats.getCounter("bytes_read")().toString))
    report += (("bytes_written", Stats.getCounter("bytes_written")().toString))

    for (qName <- queueCollection.queueNames) {
      report ++= queueCollection.stats(qName).map { case (k, v) => ("queue_" + qName + "_" + k, v) }
    }

    val summary = {
      for ((key, value) <- report) yield "STAT %s %s".format(key, value)
    }.mkString("", "\r\n", "\r\nEND")
    new MemcacheResponse(summary)
  }

  private def dumpStats() = {
    val dump = new mutable.ListBuffer[String]
    for (qName <- queueCollection.queueNames) {
      dump += "queue '" + qName + "' {"
      dump += queueCollection.stats(qName).map { case (k, v) => k + "=" + v }.mkString("  ", "\r\n  ", "")
      dump += "}"
    }
    new MemcacheResponse(dump.mkString("", "\r\n", "\r\nEND\r\n"))
  }

  private def version() = {
    new MemcacheResponse("VERSION " + Kestrel.runtime.jarVersion + "\r\n")
  }
}
