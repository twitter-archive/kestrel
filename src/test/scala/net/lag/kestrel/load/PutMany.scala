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
import scala.collection.mutable
import net.lag.extensions._


/**
 * Spam a kestrel server with 1M copies of a pop song lyric, to see how
 * quickly it can absorb them.
 */
object PutMany {
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

  private val EXPECT = ByteBuffer.wrap("STORED\r\n".getBytes)

  def put(socket: SocketChannel, queueName: String, n: Int, globalTimings: mutable.ListBuffer[Long], data: String) = {
    val spam = ByteBuffer.wrap(("set " + queueName + " 0 0 " + data.length + "\r\n" + data + "\r\n").getBytes)
    val buffer = ByteBuffer.allocate(8)
    val timings = new Array[Long](n min 100000)
    for (i <- 0 until n) {
      val startTime = System.nanoTime
      spam.rewind
      while (spam.position < spam.limit) {
        socket.write(spam)
      }
      buffer.rewind
      while (buffer.position < buffer.limit) {
        socket.read(buffer)
      }
      buffer.rewind
      EXPECT.rewind
      if (i < 100000) {
        timings(i) = System.nanoTime - startTime
      }
      if (buffer != EXPECT) {
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

  def main(args: Array[String]) = {
    if (args.length < 3) {
      Console.println("usage: put-many <clients> <count> <bytes>")
      Console.println("    spin up <clients> and put <count> items of <bytes> size into kestrel")
      System.exit(1)
    }

    val clientCount = args(0).toInt
    val totalItems = args(1).toInt
    val bytes = args(2).toInt
    val totalCount = totalItems / clientCount * clientCount
    val totalQueues = System.getProperty("queues", "1").toInt

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

    for (i <- 0 until clientCount) {
      val t = new Thread {
        override def run = {
          val socket = SocketChannel.open(new InetSocketAddress("localhost", 22133))
          val qName = "spam" + (i % totalQueues)
          put(socket, qName, totalItems / clientCount, timings, rawData.toString)
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

    val sortedTimings = timings.toList.sort { (a, b) => a < b }
    val average = sortedTimings.foldLeft(0L) { _ + _ } / sortedTimings.size.toDouble / 1000.0
    val min = sortedTimings(0) / 1000.0
    val max = sortedTimings(sortedTimings.size - 1) / 1000.0
    val maxless = sortedTimings(sortedTimings.size - 2) / 1000.0
    val maxlesser = sortedTimings(sortedTimings.size - 3) / 1000.0
    val median = (sortedTimings(sortedTimings.size / 2 - 1) + sortedTimings(sortedTimings.size / 2)) / 2000.0

    println("Transactions: min=%.2f; max=%.2f %.2f %.2f; median=%.2f; average=%.2f usec".format(min, max, maxless, maxlesser, median, average))
    var dist = Array(0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 0.9999) map { r =>
      "%.2f%%=%.2f".format(r * 100, sortedTimings((sortedTimings.size * r).toInt) / 1000.0)
    }
    println("Transactions distribution: " + dist.mkString(" "))
  }
}
