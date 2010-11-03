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

import _root_.java.net._
import _root_.java.nio._
import _root_.java.nio.channels._
import _root_.scala.collection.mutable
import _root_.net.lag.extensions._


/**
 * Spam a kestrel server with 1M copies of a pop song lyric, to see how
 * quickly it can absorb them.
 */
object Flood {
  private val DATA = "x" * 1024

  private val EXPECT = ByteBuffer.wrap("STORED\r\n".getBytes)

  def put(socket: SocketChannel, queueName: String, n: Int, data: String) = {
    val spam = ByteBuffer.wrap(("set " + queueName + " 0 0 " + data.length + "\r\n" + data + "\r\n").getBytes)
    val buffer = ByteBuffer.allocate(8)

    for (i <- 0 until n) {
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
      if (buffer != EXPECT) {
        // the "!" is important.
        throw new Exception("Unexpected response at " + i + "!")
      }
    }
  }

  def get(socket: SocketChannel, queueName: String, n: Int, data: String): Int = {
    val req = ByteBuffer.wrap(("get " + queueName + "\r\n").getBytes)
    val expectEnd = ByteBuffer.wrap("END\r\n".getBytes)
    val expectData = ByteBuffer.wrap(("VALUE " + queueName + " 0 " + data.length + "\r\n" + data + "\r\nEND\r\n").getBytes)
    val buffer = ByteBuffer.allocate(expectData.capacity)

    var count = 0
    var misses = 0
    while (count < n) {
      req.rewind
      while (req.position < req.limit) {
        socket.write(req)
      }
      buffer.rewind
      while (buffer.position < expectEnd.limit) {
        socket.read(buffer)
      }
      val oldpos = buffer.position
      buffer.flip
      expectEnd.rewind
      if (buffer == expectEnd) {
        // nothing yet. poop. :(
        misses += 1
      } else {
        buffer.position(oldpos)
        buffer.limit(buffer.capacity)
        while (buffer.position < expectData.limit) {
          socket.read(buffer)
        }
        buffer.rewind
        expectData.rewind
        if (buffer != expectData) {
          val bad = new Array[Byte](buffer.capacity)
          buffer.get(bad)
          throw new Exception("Unexpected response! thr=" + Thread.currentThread + " -> " + new String(bad))
        }
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
