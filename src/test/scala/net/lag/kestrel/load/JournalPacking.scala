/*
 * Copyright 2011 Twitter, Inc.
 * Copyright 2011 Robey Pointer <robeypointer@gmail.com>
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
import scala.annotation.tailrec
import scala.collection.mutable
import com.twitter.conversions.string._

/**
 * Seed a kestrel server with a backlog of items, then do cycles of put/get bursts to stress the
 * background journal packer.
 */
object JournalPacking extends LoadTesting {
  private val DATA = "x" * 1024

  def put(socket: SocketChannel, queueName: String, n: Int, data: String, counter: Long) = {
    val expect = client.putSuccess()
    val buffer = ByteBuffer.allocate(expect.limit)

    for (i <- 0 until n) {
      val counterData = ((counter + i).toString + data).substring(0, data.length)
      send(socket, client.put(queueName, counterData))
      if (receive(socket, buffer) != expect) {
        // the "!" is important.
        throw new Exception("Unexpected response at " + i + "!")
      }
    }
  }

  def get(socket: SocketChannel, queueName: String, n: Int, data: String, counter: Long): Int = {
    val req = client.get(queueName, Some(1000))
    val expectEnd = client.getEmpty(queueName)

    var count = 0
    var misses = 0
    while (count < n) {
      val counterData = ((counter + count).toString + data).substring(0, data.length)
      val expectData = client.getSuccess(queueName, counterData)
      val expecting = new Expecting(expectEnd, expectData)
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

  def cycle(doReads: Boolean, doWrites: Boolean) {
    val data = DATA * kilobytes
    var consumerThread: Thread = null
    var producerThread: Thread = null
    val startTime = System.currentTimeMillis

    if (doWrites) {
      producerThread = new Thread {
        override def run() = {
          val socket = SocketChannel.open(new InetSocketAddress(hostname, 22133))
          put(socket, queueName, totalItems, data, writeCounter)
        }
      }

      producerThread.start()
    }

    var misses = 0

    if (doReads) {
      consumerThread = new Thread {
        override def run() = {
          val socket = SocketChannel.open(new InetSocketAddress(hostname, 22133))
          misses = get(socket, queueName, totalItems, data, readCounter)
        }
      }
      consumerThread.start()
    }

    if (doWrites) {
      producerThread.join()
      println("Wrote %d items starting at %d.".format(totalItems, writeCounter))
      writeCounter += totalItems
    }
    if (doReads) {
      consumerThread.join()
      val duration = System.currentTimeMillis - startTime
      readCounter += totalItems
      println("Read %d items in %d msec. Consumer spun %d times in misses.".format(totalItems, duration, misses))
    }
  }

  var queueName = "spam"
  var totalItems = 25000
  var kilobytes = 1
  var pause = 1
  var cycles = 100
  var readCounter: Long = 0
  var writeCounter: Long = 0
  var hostname = "localhost"
  var port = 22133
  var client: Client = MemcacheClient
  var flushFirst = true
  var monitor = false

  def usage() {
    Console.println("usage: packing [options]")
    Console.println("    spin up a producer and consumer, write N items, then do read/write cycles ")
    Console.println("    with pauses")
    Console.println()
    Console.println("options:")
    Console.println("    -q NAME")
    Console.println("        use named queue (default: %s)".format(queueName))
    Console.println("    -n ITEMS")
    Console.println("        put ITEMS items into the queue (default: %d)".format(totalItems))
    Console.println("    -k KILOBYTES")
    Console.println("        put KILOBYTES per queue item (default: %d)".format(kilobytes))
    Console.println("    -t SECONDS")
    Console.println("        pause SECONDS between cycles (default: %d)".format(pause))
    Console.println("    -c CYCLES")
    Console.println("        do read/writes CYCLES times (default: %d)".format(cycles))
    Console.println("    -h HOSTNAME")
    Console.println("        use kestrel on HOSTNAME (default: %s)".format(hostname))
    Console.println("    -p PORT")
    Console.println("        use kestrel on PORT (default: %d)".format(port))
    Console.println("    --thrift")
    Console.println("        use thrift RPC")
    Console.println("    -F")
    Console.println("        don't flush queue(s) before the test")
    Console.println("    -M")
    Console.println("        monitor queue stats during the test")
  }

  @tailrec
  def parseArgs(args: List[String]): Unit = args match {
    case Nil =>
    case "--help" :: xs =>
      usage()
      System.exit(0)
    case "-q" :: x :: xs =>
      queueName = x
      parseArgs(xs)
    case "-n" :: x :: xs =>
      totalItems = x.toInt
      parseArgs(xs)
    case "-k" :: x :: xs =>
      kilobytes = x.toInt
      parseArgs(xs)
    case "-t" :: x :: xs =>
      pause = x.toInt
      parseArgs(xs)
    case "-c" :: x :: xs =>
      cycles = x.toInt
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
    case "-M" :: xs =>
      monitor = true
      parseArgs(xs)
    case _ =>
      usage()
      System.exit(1)
  }

  def main(args: Array[String]) = {
    parseArgs(args.toList)

    println("packing: " + totalItems + " items of " + kilobytes + "kB with " + pause + " second pauses")

    // flush queues first
    if (flushFirst) {
      println("Flushing queues first.")
      val socket = tryHard { SocketChannel.open(new InetSocketAddress(hostname, port)) }
      send(socket, client.flush(queueName))
      expect(socket, client.flushSuccess())
      socket.close()
    }

    if (monitor) monitorQueue(hostname, queueName)

    cycle(false, true)
    for (i <- 0 until cycles) {
      println("cycle: " + (i + 1))
      cycle(true, true)
      Thread.sleep(pause * 1000)
    }
    cycle(true, false)
  }
}
