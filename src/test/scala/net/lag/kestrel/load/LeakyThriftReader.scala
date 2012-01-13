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
import java.util.concurrent.atomic._
import scala.collection.mutable.BitSet
import scala.util.Random
import com.twitter.conversions.string._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.thrift.{ThriftClientFramedCodec, ThriftClientRequest}
import com.twitter.util.Time
import net.lag.kestrel.thrift._
import org.apache.thrift.protocol.TBinaryProtocol

/**
  * Have one producer generate data at a steady rate, while several clients
  * consume them with reliable reads, occassionally failing to confirm an
  * item. Verify that all items are eventually read with confirmation.
  */
object LeakyThriftReader {
  private val DATA_TEMPLATE = "%08x"

  def put(client: Kestrel.FinagledClient, queueName: String, id: Int) = {
    val data = DATA_TEMPLATE.format(id)

    val putRequest = client.put(queueName, List[ByteBuffer](ByteBuffer.wrap(data.getBytes)))
    if (putRequest() != 1) {
      // the "!" is important.
      throw new Exception("Unexpected response at " + id + "!")
    }
  }

  def get(client: Kestrel.FinagledClient, queueName: String): Option[(Long, Int)] = {
    val getRequest = client.get(queueName, 1, 100, 5000)
    val items = getRequest()

    if (items.isEmpty) {
      None
    } else {
      if (items.length > 1) {
        // the "!" is important.
        throw new Exception("Unexpected number of items: %d!".format(items.length))
      }
      val item = items.head
      val bytes = new Array[Byte](item.data.remaining)
      item.data.get(bytes)
      val bitId = java.lang.Integer.parseInt(new String(bytes), 16)
      if (verbose) println("%s: open %016x = %08x".format(Time.now, item.id, bitId))
      Some((item.id, bitId))
    }
  }

  def confirm(client: Kestrel.FinagledClient, queueName: String, id: Long) = {
    if (verbose) println("%s: confirm %016x".format(Time.now, id))
    val confirmRequest = client.confirm(queueName, Set(id))
    val confirmedCount = confirmRequest()

    if (confirmedCount != 1) {
      // the "!" is important.
      throw new Exception("Unexpected number of items confirmed: %d (%d)!".format(confirmedCount, id))
    }
  }

  var totalItems = 1000
  var dropRate = 0.01
  var queueName = "spam"
  var hostname = "localhost"
  var port = 2229
  var threads = 1
  var flushFirst = true
  var verbose = false

  def usage() {
    Console.println("usage: leaky-reader [options]")
    Console.println("    spin up a producer and consumer(s) and deliver N items through kestrel, with occassional drops")
    Console.println()
    Console.println("options:")
    Console.println("    -n ITEMS")
    Console.println("        put ITEMS items into the queue (default: %d)".format(totalItems))
    Console.println("    -d RATE")
    Console.println("        drop RATE items (fractional percentage, default: %f)".format(dropRate))
    Console.println("    -q NAME")
    Console.println("        use queue NAME (default: %s)".format(queueName))
    Console.println("    -t THREADS")
    Console.println("        create THREADS consumers (default: %d)".format(threads))
    Console.println("    -h HOSTNAME")
    Console.println("        use kestrel on HOSTNAME (default: %s)".format(hostname))
    Console.println("    -p PORT")
    Console.println("        use kestrel on PORT (default: %d)".format(port))
    Console.println("    -F")
    Console.println("        don't flush queue(s) before the test")
    Console.println("    -v")
    Console.println("        be verbose, writing opens/confirms to stdout")
  }

  def parseArgs(args: List[String]): Unit = args match {
    case Nil =>
    case "--help" :: xs =>
      usage()
      System.exit(0)
    case "-n" :: x :: xs =>
      totalItems = x.toInt
      parseArgs(xs)
    case "-d" :: x :: xs =>
      dropRate = x.toFloat
      parseArgs(xs)
    case "-q" :: x :: xs =>
      queueName = x
      parseArgs(xs)
    case "-t" :: x :: xs =>
      threads = x.toInt
      parseArgs(xs)
    case "-h" :: x :: xs =>
      hostname = x
      parseArgs(xs)
    case "-p" :: x :: xs =>
      port = x.toInt
      parseArgs(xs)
    case "-F" :: xs =>
      flushFirst = false
      parseArgs(xs)
    case "-v" :: xs =>
      verbose = true
      parseArgs(xs)
    case _ =>
      usage()
      System.exit(1)
  }

  def main(args: Array[String]) = {
    parseArgs(args.toList)

    val service: Service[ThriftClientRequest, Array[Byte]] = ClientBuilder()
      .hosts("%s:%d".format(hostname, port))
      .codec(ThriftClientFramedCodec())
      .hostConnectionLimit(threads * 5)
      .hostConnectionCoresize(threads)
      .retries(5)
      .build()

    val client = new Kestrel.FinagledClient(service, new TBinaryProtocol.Factory())

    println("leaky-reader: %d threads each sending %d items through %s".format(
      threads, totalItems, queueName))

    // flush queues first
    if (flushFirst) {
      println("Flushing queues first.")
      client.flushQueue(queueName)()
    }

    val producerThread = new Thread {
      override def run = {
        var n = 0
        while(n < totalItems) {
          put(client, queueName, n)
          n += 1
        }
      }
    }

    val rng = new Random()
    var threadList = List[Thread](producerThread)
    val numRead = new AtomicInteger(0)
    val incompleteReads = new BitSet(totalItems)
    for (i <- 0 until totalItems) {
      incompleteReads.add(i)
    }
    for (i <- 0 until threads) {
      val consumerThread = new Thread {
        override def run = {
          while (numRead.get < totalItems) {
            val resultOption = get(client, queueName)
            resultOption match {
              case Some((xid, bitId)) =>
                if (rng.nextFloat >= dropRate) {
                  confirm(client, queueName, xid)
                  val x = numRead.incrementAndGet
                  incompleteReads.synchronized { incompleteReads.remove(bitId) }
                  if (x % 1000 == 0) println(x)
                }
              case None => // a miss, a palpable miss
            }
          }
        }
      }

      threadList = consumerThread :: threadList
    }

    val startTime = System.currentTimeMillis
    threadList.foreach { _.start() }
    threadList.foreach { _.join() }
    val duration = System.currentTimeMillis - startTime

    service.release()

    println("Finished in %d msec (%.1f usec/put throughput).".format(duration, duration * 1000.0 / (totalItems * threads)))
    if (incompleteReads.nonEmpty) {
      println("Missed the following items:")
      incompleteReads.foreach { i => println("\t%08x".format(i)) }
    } else {
      println("Completed all reads")
    }
  }
}
