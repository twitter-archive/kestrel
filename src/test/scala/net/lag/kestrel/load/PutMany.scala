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

import java.net._
import java.nio._
import java.nio.channels._
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import com.twitter.conversions.string._

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

  def put(socket: SocketChannel, client: Client, queueName: String, n: Int, globalTimings: mutable.ListBuffer[Long], data: String) = {
    val spam = client.put(queueName, data)
    val expect = client.putSuccess()

    val buffer = ByteBuffer.allocate(expect.capacity)
    val timings = new Array[Long](n min 100000)
    for (i <- 0 until n) {
      val startTime = System.nanoTime
      send(socket, spam)
      receive(socket, buffer)
      if (i < 100000) {
        timings(i) = System.nanoTime - startTime
      }
      expect.rewind()
      if (buffer != expect) {
        // the "!" is important.
        throw new Exception("Unexpected response at " + i + "!")
      }
    }

    // coalesce timings
    globalTimings.synchronized {
      if (globalTimings.size < 100000) {
        globalTimings ++= timings.toList
      }
    }
  }

  var clientCount = 100
  var totalItems = 10000
  var bytes = 1024
  var queueName = "spam"
  var queueCount = 1
  var hostname = "localhost"
  var port = 22133
  var client: Client = MemcacheClient
  var flushFirst = true

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
    case _ =>
      usage()
      System.exit(1)
  }

  val failedConnects = new AtomicInteger

  def tryHard[A](f: => A): A = {
    try {
      f
    } catch {
      case e: java.io.IOException =>
        failedConnects.incrementAndGet()
        tryHard(f)
    }
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

    var threadList: List[Thread] = Nil
    val startTime = System.currentTimeMillis
    val timings = new mutable.ListBuffer[Long]

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

    println("Put %d items of %d bytes to %s:%d in %d queues named %s using %d clients.".format(
      totalItems, bytes, hostname, port, queueCount, queueName, clientCount))

    for (i <- 0 until clientCount) {
      val t = new Thread {
        override def run = {
          val socket = tryHard { SocketChannel.open(new InetSocketAddress(hostname, port)) }
          val qName = queueName + (if (queueCount > 1) (i % queueCount).toString else "")
          put(socket, client, qName, totalItems / clientCount, timings, rawData.toString)
        }
      }
      threadList = t :: threadList
      t.start
    }
    for (t <- threadList) {
      t.join
    }

    val duration = System.currentTimeMillis - startTime
    Console.println("Finished in %d msec (%.1f usec/put throughput).".format(duration, duration * 1000.0 / totalCount))
    if (failedConnects.get > 0) {
      println("Had to retry %d times to make all connections.".format(failedConnects.get))
    }
    val sortedTimings = timings.toList.sorted
    val average = sortedTimings.foldLeft(0L) { _ + _ } / sortedTimings.size.toDouble / 1000.0
    val min = sortedTimings(0) / 1000.0
    val max = sortedTimings(sortedTimings.size - 1) / 1000.0
    val maxless = sortedTimings(sortedTimings.size - 2) / 1000.0
    val maxlesser = sortedTimings(sortedTimings.size - 3) / 1000.0
    val median = (sortedTimings(sortedTimings.size / 2 - 1) + sortedTimings(sortedTimings.size / 2)) / 2000.0

    println("Transactions: min=%.2f; max=%.2f %.2f %.2f; median=%.2f; average=%.2f usec".format(min, max, maxless, maxlesser, median, average))
    val dist = Array(0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9999) map { r =>
      "%.2f%%=%.2f".format(r * 100, sortedTimings((sortedTimings.size * r).toInt) / 1000.0)
    }
    println("Transactions distribution: " + dist.mkString(" "))
  }
}
