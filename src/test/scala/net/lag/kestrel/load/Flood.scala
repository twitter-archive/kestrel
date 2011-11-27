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
 * Flood a kestrel server with a bunch of puts and gets, and time how long it takes for them to
 * filter through.
 */
object Flood extends LoadTesting {
  private val DATA = "x" * 1024

  def put(socket: SocketChannel, queueName: String, n: Int, data: String) = {
    val spam = if (rollup == 1) {
      client.put(queueName, data)
    } else {
      client.putN(queueName, (0 until rollup).map { _ => data })
    }
    val expect = if (rollup == 1) client.putSuccess() else client.putNSuccess(rollup)
    val buffer = ByteBuffer.allocate(expect.limit)

    var i = 0
    while (i < n) {
      send(socket, spam)
      if (receive(socket, buffer) != expect) {
        // the "!" is important.
        throw new Exception("Unexpected response at " + i + "!")
      }
      i += rollup
    }
  }

  def get(socket: SocketChannel, queueName: String, n: Int, data: String): Int = {
    val req = if (rollup == 1) {
      client.get(queueName, if (blockingReads) Some(1000) else None)
    } else {
      client.monitor(queueName, 1000, rollup)
    }
    val expectNoData = client.getEmpty(queueName)
    val expectData = client.getSuccess(queueName, data)
    val expecting = new Expecting(expectNoData, expectData)
    val monitorSuccess = client.monitorSuccess(queueName, (0 until rollup).map { _ => data })
    val buffer = ByteBuffer.allocate(monitorSuccess.limit)
    val confirm = client.confirmMonitor(queueName, rollup)
    val confirmSuccess = client.confirmMonitorSuccess(queueName, rollup)
    val confirmSuccessBuffer = ByteBuffer.allocate(confirmSuccess.limit)

    var count = 0
    var misses = 0
    var leftInCycle = 0
    while (count < n) {
      if (leftInCycle == 0) {
        send(socket, req)
        leftInCycle = rollup
      }
      if (rollup == 1 || client.monitorHasMultipleResponses) {
        val got = expecting(socket)
        if (got == expectNoData) {
          // nothing yet. poop. :(
          misses += 1
          leftInCycle = 0
        } else {
          count += 1
          leftInCycle -= 1
          if (leftInCycle == 0 && rollup > 1) {
            if (expecting(socket) != expectNoData) {
              throw new Exception("Unexpected monitor response at " + count + "!")
            }
            send(socket, confirm)
            if (receive(socket, confirmSuccessBuffer) != confirmSuccess) {
              throw new Exception("Unexpected confirm response at " + count + "!")
            }
          }
        }
      } else {
        if (receive(socket, buffer) != monitorSuccess) {
          throw new Exception("Unexpected response at " + count + "!")
        }
        count += rollup
        leftInCycle -= rollup
      }
    }
    misses
  }

  var totalItems = 10000
  var kilobytes = 1
  var queueName = "spam"
  var prefillItems = 0
  var rollup = 1
  var hostname = "localhost"
  var port = 22133
  var threads = 1
  var blockingReads = false
  var client: Client = MemcacheClient
  var flushFirst = true

  def usage() {
    Console.println("usage: flood [options]")
    Console.println("    spin up a producer and consumer and flood N items through kestrel")
    Console.println()
    Console.println("options:")
    Console.println("    -n ITEMS")
    Console.println("        put ITEMS items into the queue (default: %d)".format(totalItems))
    Console.println("    -k KILOBYTES")
    Console.println("        put KILOBYTES per queue item (default: %d)".format(kilobytes))
    Console.println("    -q NAME")
    Console.println("        use queue NAME (default: %s)".format(queueName))
    Console.println("    -P ITEMS")
    Console.println("        prefill ITEMS items into the queue before the test (default: %d)".format(prefillItems))
    Console.println("    -t THREADS")
    Console.println("        create THREADS producers and THREADS consumers (default: %d)".format(threads))
    Console.println("    -r ITEMS")
    Console.println("        roll up ITEMS items into a single multi-put request (default: %d)".format(rollup))
    Console.println("    -h HOSTNAME")
    Console.println("        use kestrel on HOSTNAME (default: %s)".format(hostname))
    Console.println("    -p PORT")
    Console.println("        use kestrel on PORT (default: %d)".format(port))
    Console.println("    -B")
    Console.println("        do blocking reads (reads with a timeout)")
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
    case "-n" :: x :: xs =>
      totalItems = x.toInt
      parseArgs(xs)
    case "-k" :: x :: xs =>
      kilobytes = x.toInt
      parseArgs(xs)
    case "-q" :: x :: xs =>
      queueName = x
      parseArgs(xs)
    case "-P" :: x :: xs =>
      prefillItems = x.toInt
      parseArgs(xs)
    case "-t" :: x :: xs =>
      threads = x.toInt
      parseArgs(xs)
    case "-r" :: x :: xs =>
      rollup = x.toInt
      parseArgs(xs)
    case "-h" :: x :: xs =>
      hostname = x
      parseArgs(xs)
    case "-p" :: x :: xs =>
      port = x.toInt
      parseArgs(xs)
    case "-B" :: xs =>
      blockingReads = true
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

  def main(args: Array[String]) = {
    parseArgs(args.toList)
    val data = DATA * kilobytes

    // flush queues first
    if (flushFirst) {
      println("Flushing queues first.")
      val socket = tryHard { SocketChannel.open(new InetSocketAddress(hostname, port)) }
      send(socket, client.flush(queueName))
      expect(socket, client.flushSuccess())
      socket.close()
    }

    if (prefillItems > 0) {
      println("prefill: " + prefillItems + " items of " + kilobytes + "kB")
      val socket = tryHard { SocketChannel.open(new InetSocketAddress(hostname, port)) }
      put(socket, queueName, prefillItems, data)
    }

    println("flood: %d threads each sending %d items of %dkB through %s".format(
      threads, totalItems, kilobytes, queueName))

    var threadList: List[Thread] = Nil
    val misses = new AtomicInteger

    for (i <- 0 until threads) {
      val producerThread = new Thread {
        override def run = {
          val socket = tryHard { SocketChannel.open(new InetSocketAddress(hostname, port)) }
          put(socket, queueName, totalItems, data)
        }
      }

      val consumerThread = new Thread {
        override def run = {
          val socket = tryHard { SocketChannel.open(new InetSocketAddress(hostname, port)) }
          val n = get(socket, queueName, totalItems, data)
          misses.addAndGet(n)
        }
      }

      threadList = producerThread :: consumerThread :: threadList
    }

    val startTime = System.currentTimeMillis
    threadList.foreach { _.start() }
    threadList.foreach { _.join() }
    val duration = System.currentTimeMillis - startTime

    println("Finished in %d msec (%.1f usec/put throughput).".format(duration, duration * 1000.0 / (totalItems * threads)))
    println("Consumer(s) spun %d times in misses.".format(misses.get))
  }
}
