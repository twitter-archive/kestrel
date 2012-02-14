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

package net.lag.kestrel.load

import com.twitter.conversions.string._
import com.twitter.ostrich.stats.Histogram
import com.twitter.util.{Duration, Time}
import java.net._
import java.nio._
import java.nio.channels._
import scala.collection.mutable

/**
 * Spam a kestrel server with 1M copies of a pop song lyric, to see how
 * quickly it can absorb them.
 */
object PutMany extends LoadTesting {
  private val LYRIC =
"crossed off, but never forgotten\n" +
"misplaced, but never losing hold\n" +
"these are the moments that bind us\n" +
"repressed, but never erased\n" +
"knocked down, but never giving up\n" +
"locked up where no one can find us\n" +
"we'll survive in here til the end\n" +
"\n" +
"there are no more fights to fight\n" +
"my trophies are the scars that will never heal\n" +
"but i get carried away sometimes\n" +
"i wake up in the night swinging at the ceiling\n" +
"it's hard to leave old ways behind\n" +
"but harder when you think that's all there is\n" +
"don't look at me that way\n" +
"\n" +
"ignored, when your whole world's collapsed\n" +
"dismissed, before you speak a word\n" +
"these are the moments that bind you\n" +
"come clean, but everything's wrong\n" +
"sustained, but barely holding on\n" +
"run down, with no one to find you\n" +
"we're survivors, here til the end\n"

  case class Session(socket: SocketChannel, queueName: String, count: Int, timings: Array[Int])

  def put(session: Session, data: String) = {
    val spam = if (rollup == 1) {
      client.put(queueName, data)
    } else {
      client.putN(queueName, (0 until rollup).map { _ => data })
    }
    val expect = if (rollup == 1) client.putSuccess() else client.putNSuccess(rollup)

    val buffer = ByteBuffer.allocate(expect.capacity)

    var timingCounter = 0
    var i = 0
    while (i < session.count) {
      val startTime = Time.now
      send(session.socket, spam)
      receive(session.socket, buffer)
      if (timingCounter < 100000) {
        session.timings(timingCounter) = (Time.now - startTime).inMilliseconds.toInt
      }
      expect.rewind()
      if (buffer != expect) {
        // the "!" is important.
        throw new Exception("Unexpected response at " + i + "!")
      }
      i += rollup
      timingCounter += 1
    }
  }

  def get(session: Session, data: String) = {
    val spam = if (rollup == 1) client.get(queueName, None) else client.getN(queueName, None, rollup)
    val expect = if (rollup == 1) {
      client.getSuccess(queueName, data)
    } else {
      client.getNSuccess(queueName, (0 until rollup).map { _ => data })
    }

    val buffer = ByteBuffer.allocate(expect.capacity)

    var timingCounter = 0
    var i = 0
    while (i < session.count) {
      val startTime = Time.now
      send(session.socket, spam)
      receive(session.socket, buffer)
      if (timingCounter < 100000) {
        session.timings(timingCounter) = (Time.now - startTime).inMilliseconds.toInt
      }
      expect.rewind()
      if (buffer != expect) {
        // the "!" is important.
        throw new Exception("Unexpected response at " + i + "!")
      }
      i += rollup
      timingCounter += 1
    }
  }

  var clientCount = 100
  var totalItems = 10000
  var bytes = 1024
  var rollup = 1
  var queueName = "spam"
  var queueCount = 1
  var hostname = "localhost"
  var port = 22133
  var client: Client = MemcacheClient
  var flushFirst = true
  var getAlso = false
  var graph = false
  var graphYMax: Option[Int] = None

  def usage() {
    Console.println("usage: put-many [options]")
    Console.println("    spam items into kestrel")
    Console.println()
    Console.println("options:")
    Console.println("    -c CLIENTS")
    Console.println("        use CLIENTS concurrent clients (default: %d)".format(clientCount))
    Console.println("    -n ITEMS")
    Console.println("        put ITEMS items into the queue (default: %d)".format(totalItems))
    Console.println("    -b BYTES")
    Console.println("        put BYTES per queue item (default: %d)".format(bytes))
    Console.println("    -r ITEMS")
    Console.println("        roll up ITEMS items into a single multi-put/multi-get request (default: %d)".format(rollup))
    Console.println("    -q NAME")
    Console.println("        use queue NAME as prefix (default: %s)".format(queueName))
    Console.println("    -Q QUEUES")
    Console.println("        use QUEUES different queues (default: %d)".format(queueCount))
    Console.println("    -h HOSTNAME")
    Console.println("        use kestrel on HOSTNAME (default: %s)".format(hostname))
    Console.println("    -p PORT")
    Console.println("        use kestrel on PORT (default: %d)".format(port))
    Console.println("    --thrift")
    Console.println("        use thrift RPC")
    Console.println("    -F")
    Console.println("        don't flush queue(s) before the test")
    Console.println("    -g")
    Console.println("        get the items afterwards")
  }

  def parseArgs(args: List[String]): Unit = args match {
    case Nil =>
    case "--help" :: xs =>
      usage()
      System.exit(0)
    case "-c" :: x :: xs =>
      clientCount = x.toInt
      parseArgs(xs)
    case "-n" :: x :: xs =>
      totalItems = x.toInt
      parseArgs(xs)
    case "-b" :: x :: xs =>
      bytes = x.toInt
      parseArgs(xs)
    case "-r" :: x :: xs =>
      rollup = x.toInt
      parseArgs(xs)
    case "-q" :: x :: xs =>
      queueName = x
      parseArgs(xs)
    case "-Q" :: x :: xs =>
      queueCount = x.toInt
      parseArgs(xs)
    case "-h" :: x :: xs =>
      hostname = x
      parseArgs(xs)
    case "-p" :: x :: xs =>
      port = x.toInt
      parseArgs(xs)
    case "--thrift" :: xs =>
      client = ThriftClient
      port = 2229
      parseArgs(xs)
    case "-F" :: xs =>
      flushFirst = false
      parseArgs(xs)
    case "-g" :: xs =>
      getAlso = true
      parseArgs(xs)
    case "--graph" :: xs =>
      graph = true
      parseArgs(xs)
    case "--ymax" :: x :: xs =>
      graphYMax = Some(x.toInt)
      parseArgs(xs)
    case _ =>
      usage()
      System.exit(1)
  }

  def showHistogram(timings: Seq[Array[Int]]) {
    val h = new Histogram()
    for (t1 <- timings; t2 <- t1) h.add(t2)
    val stats = Seq(
      "min" -> h.minimum,
      "max" -> h.maximum,
      "p50" -> h.getPercentile(0.5),
      "p75" -> h.getPercentile(0.75),
      "p90" -> h.getPercentile(0.9),
      "p95" -> h.getPercentile(0.95),
      "p99" -> h.getPercentile(0.99),
      "p999" -> h.getPercentile(0.999),
      "p9999" -> h.getPercentile(0.9999)
    )
    if (graph) new Grapher(h, (1.0 / 1000000), 25, graphYMax).draw()
    println("Distribution in usec: " + stats.map { case (k, v) => k + "=" + v }.mkString(" "))
  }

  def cycle(f: Session => Unit) {
    val totalCount = totalItems / clientCount * clientCount
    val itemsPerClient = totalItems / clientCount

    val timings = (0 until clientCount).map { _ => new Array[Int](itemsPerClient min 100000) }.toSeq

    val threadList = (0 until clientCount).map { i =>
      new Thread {
        override def run = {
          val socket = tryHard { SocketChannel.open(new InetSocketAddress(hostname, port)) }
          val qName = queueName + (if (queueCount > 1) (i % queueCount).toString else "")
          f(Session(socket, qName, itemsPerClient, timings(i)))
        }
      }
    }.toSeq

    val startTime = Time.now
    threadList.foreach { _.start() }
    threadList.foreach { _.join() }
    val duration = (Time.now - startTime).inMilliseconds

    Console.println("Finished in %d msec (%.1f usec/put throughput).".format(duration, duration * 1000.0 / totalCount))
    if (failedConnects.get > 0) {
      println("Had to retry %d times to make all connections.".format(failedConnects.get))
    }
    showHistogram(timings)
  }

  def main(args: Array[String]) = {
    parseArgs(args.toList)

    val totalCount = totalItems / clientCount * clientCount

    val rawData = new StringBuilder
    while (rawData.size < bytes) {
      val remaining = bytes - rawData.size
      if (remaining > LYRIC.size) {
        rawData append LYRIC
      } else {
        rawData append LYRIC.substring(0, remaining)
      }
    }

    // flush queues first
    if (flushFirst) {
      println("Flushing queues first.")
      val socket = tryHard { SocketChannel.open(new InetSocketAddress(hostname, port)) }
      for (i <- 0 until queueCount) {
        val name = queueName + (if (queueCount > 1) (i % queueCount).toString else "")
        send(socket, client.flush(name))
        expect(socket, client.flushSuccess())
      }
      socket.close()
    }

    println("Put %d items of %d bytes in bursts of %d to %s:%d in %d queues named %s using %d clients.".format(
      totalItems, bytes, rollup, hostname, port, queueCount, queueName, clientCount))
    cycle { session =>
      put(session, rawData.toString)
    }

    if (getAlso) {
      println("Get %d items of %d bytes in batches of %d from %s:%d in %d queues named %s using %d clients.".format(
         totalItems, bytes, rollup, hostname, port, queueCount, queueName, clientCount))
      cycle { session =>
        get(session, rawData.toString)
      }
    }
  }
}
