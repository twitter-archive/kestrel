/*
 * Copyright 2009 Twitter, Inc.
 * Copyright 2009 Robey Pointer <robeypointer@lag.net>
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
"we're survivors, here til the end"

  private val EXPECT = ByteBuffer.wrap("STORED\r\n".getBytes)

  def put(socket: SocketChannel, queueName: String, n: Int, globalTimings: mutable.ListBuffer[Long]) = {
    val spam = ByteBuffer.wrap(("set " + queueName + " 0 0 " + LYRIC.length + "\r\n" + LYRIC + "\r\n").getBytes)
    val buffer = ByteBuffer.allocate(8)
    val timings = new Array[Long](n)
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
      timings(i) = System.nanoTime - startTime
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
    if (args.length < 1) {
      Console.println("usage: put-many <N>")
      Console.println("    spin up N clients and put 10k items spread across N queues")
      System.exit(1)
    }

    val clientCount = args(0).toInt
    val totalCount = 10000 / clientCount * clientCount

    var threadList: List[Thread] = Nil
    val startTime = System.currentTimeMillis
    val timings = new mutable.ListBuffer[Long]

    for (i <- 0 until clientCount) {
      val t = new Thread {
        override def run = {
          val socket = SocketChannel.open(new InetSocketAddress("localhost", 22133))
          put(socket, "spam", 10000 / clientCount, timings)
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
    val median = (sortedTimings(sortedTimings.size / 2 - 1) + sortedTimings(sortedTimings.size / 2)) / 2000.0

    println("Transactions: min=%.2f; max=%.2f; median=%.2f; average=%.2f usec".format(min, max, median, average))
    var dist = Array(0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99) map { r =>
      "%d%%=%.2f".format((r * 100).toInt, sortedTimings((sortedTimings.size * r).toInt) / 1000.0)
    }
    println("Transactions distribution: " + dist.mkString(" "))
  }
}
