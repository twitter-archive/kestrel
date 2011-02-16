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
import com.twitter.conversions.string._

/**
 * Spam a kestrel server with 1M copies of a pop song lyric, to see how
 * quickly it can absorb them.
 */
object Flood extends LoadTesting {
  private val DATA = "x" * 1024

  private val EXPECT = ByteBuffer.wrap("STORED\r\n".getBytes)

  def put(socket: SocketChannel, queueName: String, n: Int, data: String) = {
    val spam = ByteBuffer.wrap(("set " + queueName + " 0 0 " + data.length + "\r\n" + data + "\r\n").getBytes)
    val buffer = ByteBuffer.allocate(EXPECT.limit)

    for (i <- 0 until n) {
      send(socket, spam)
      if (receive(socket, buffer) != EXPECT) {
        // the "!" is important.
        throw new Exception("Unexpected response at " + i + "!")
      }
    }
  }

  def get(socket: SocketChannel, queueName: String, n: Int, data: String): Int = {
    val req = ByteBuffer.wrap(("get " + queueName + "\r\n").getBytes)
    val expectEnd = ByteBuffer.wrap("END\r\n".getBytes)
    val expectData = ByteBuffer.wrap(("VALUE " + queueName + " 0 " + data.length + "\r\n" + data + "\r\nEND\r\n").getBytes)
    val expecting = new Expecting(expectEnd, expectData)

    var count = 0
    var misses = 0
    while (count < n) {
      send(socket, req)
      val got = expecting(socket)
      if (got == expectEnd) {
        // nothing yet. poop. :(
        misses += 1
      } else {
        count += 1
      }
    }
    misses
  }

  var totalItems = 10000
  var kilobytes = 1

  def usage() {
    Console.println("usage: flood [options]")
    Console.println("    spin up a producer and consumer and flood N items through kestrel")
    Console.println()
    Console.println("options:")
    Console.println("    -n ITEMS")
    Console.println("        put ITEMS items into the queue (default: %d)".format(totalItems))
    Console.println("    -k KILOBYTES")
    Console.println("        put KILOBYTES per queue item (default: %d)".format(kilobytes))
  }

  def parseArgs(args: List[String]): Unit = args match {
    case Nil =>
    case "--help" :: xs =>
      usage()
      System.exit(0)
    case "-n" :: x :: xs =>
      totalItems = x.toInt
      parseArgs(xs)
    case "-k" :: x :: xs =>
      kilobytes = x.toInt
      parseArgs(xs)
    case _ =>
      usage()
      System.exit(1)
  }

  def main(args: Array[String]) = {
    parseArgs(args.toList)
    val data = DATA * kilobytes

    println("flood: " + totalItems + " items of " + kilobytes + "kB")

    val producerThread = new Thread {
      override def run = {
        val socket = SocketChannel.open(new InetSocketAddress("localhost", 22133))
        val qName = "spam"
        put(socket, qName, totalItems, data)
      }
    }
    val consumerThread = new Thread {
      var misses = 0
      override def run = {
        val socket = SocketChannel.open(new InetSocketAddress("localhost", 22133))
        val qName = "spam"
        misses = get(socket, qName, totalItems, data)
      }
    }

    val startTime = System.currentTimeMillis
    producerThread.start
    consumerThread.start
    producerThread.join
    consumerThread.join
    val duration = System.currentTimeMillis - startTime
    println("Finished in %d msec (%.1f usec/put throughput).".format(duration, duration * 1000.0 / totalItems))
    println("Consumer spun %d times in misses.".format(consumerThread.misses))
  }
}
