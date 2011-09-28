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
import java.util.concurrent.atomic._
import scala.util.Random
import com.twitter.conversions.string._

/**
 * Have one producer trickle in pop lyrics at a steady but slow rate, while
 * many clients clamor for each one. This is similar to how queues operate in
 * some typical production environments.
 */
object ManyClients {
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

  private val got = new AtomicInteger(0)


  def put(sleep: Int, socket: SocketChannel, queueName: String, n: Int) = {
    val spam = ByteBuffer.wrap(("set " + queueName + " 0 0 " + LYRIC.length + "\r\n" + LYRIC + "\r\n").getBytes)
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
      if (buffer != EXPECT) {
        // the "!" is important.
        throw new Exception("Unexpected response at " + i + "!")
      }
      if (sleep > 0) Thread.sleep(sleep)
    }
  }

  def getStuff(count: Int, hostname: String, port: Int, queueName: String, useTransactions: Boolean, killPercent: Int) = {
    var socket: SocketChannel = null
    val xact = if (useTransactions) "/close/open" else ""
    val req = ByteBuffer.wrap(("get " + queueName + "/t=1000" + xact + "\r\n").getBytes)
    val expectEnd = ByteBuffer.wrap("END\r\n".getBytes)
    val expectLyric = ByteBuffer.wrap(("VALUE " + queueName + " 0 " + LYRIC.length + "\r\n" + LYRIC + "\r\nEND\r\n").getBytes)
    val buffer = ByteBuffer.allocate(expectLyric.capacity)
    expectLyric.rewind

    while (got.get < count) {
      if (socket eq null) {
        socket = SocketChannel.open(new InetSocketAddress(hostname, port))
      }
      req.rewind
      while (req.position < req.limit) {
        socket.write(req)
      }

      if (Random.nextInt(100) < killPercent) {
        try {
          socket.close()
        } catch {
          case e: Exception =>
        }
        socket = null
      } else {
        buffer.rewind
        while (buffer.position < expectEnd.limit) {
          socket.read(buffer)
        }
        val oldpos = buffer.position
        buffer.flip
        expectEnd.rewind
        if (buffer == expectEnd) {
          // i am not the winner. poop. :(
        } else {
          buffer.position(oldpos)
          buffer.limit(buffer.capacity)
          while (buffer.position < expectLyric.limit) {
            socket.read(buffer)
          }
          buffer.rewind
          expectLyric.rewind
          if (buffer != expectLyric) {
            val bad = new Array[Byte](buffer.capacity)
            buffer.get(bad)
            throw new Exception("Unexpected response! thr=" + Thread.currentThread + " -> " + new String(bad))
          }
          got.incrementAndGet
        }
      }
    }

    if ((socket ne null) && useTransactions) {
      val finish = ByteBuffer.wrap(("get " + queueName + "/close\r\n").getBytes)
      while (finish.position < finish.limit) {
        socket.write(finish)
      }
      buffer.rewind
      while (buffer.position < expectEnd.limit) {
        socket.read(buffer)
      }
      buffer.flip()
      expectEnd.rewind()
      if (buffer != expectEnd) {
        val bad = new Array[Byte](buffer.capacity)
        buffer.get(bad)
        throw new Exception("Unexpected response to final close! thr=" + Thread.currentThread + " -> " + new String(bad))
      }
    }
  }

  var sleep = 100
  var count = 100
  var clientCount = 100
  var hostname = "localhost"
  var dropPercent = 0
  var killPercent = 0
  var useTransactions = false

  def usage() {
    Console.println("usage: many-clients [options]")
    Console.println("    spin up N clients and have them do timeout reads on a queue while a")
    Console.println("    single producer trickles out.")
    Console.println()
    Console.println("options:")
    Console.println("    -s MILLESCONDS")
    Console.println("        sleep MILLISECONDS between puts (default: %d)".format(sleep))
    Console.println("    -n ITEMS")
    Console.println("        put ITEMS total items into the queue (default: %d)".format(count))
    Console.println("    -c CLIENTS")
    Console.println("        use CLIENTS consumers (default: %d)".format(clientCount))
    Console.println("    -h HOSTNAME")
    Console.println("        use kestrel on HOSTNAME (default: %s)".format(hostname))
    Console.println("    -k PERCENT")
    Console.println("        kill PERCENT %% of clients before they can read the response (default: %d)".format(dropPercent))
    Console.println("    -x")
    Console.println("        use transactional gets")
  }

  def parseArgs(args: List[String]): Unit = args match {
    case Nil =>
    case "--help" :: xs =>
      usage()
      System.exit(0)
    case "-s" :: x :: xs =>
      sleep = x.toInt
      parseArgs(xs)
    case "-n" :: x :: xs =>
      count = x.toInt
      parseArgs(xs)
    case "-c" :: x :: xs =>
      clientCount = x.toInt
      parseArgs(xs)
    case "-h" :: x :: xs =>
      hostname = x
      parseArgs(xs)
    case "-k" :: x :: xs =>
      killPercent = x.toInt
      parseArgs(xs)
    case "-x" :: xs =>
      useTransactions = true
      parseArgs(xs)
    case _ =>
      usage()
      System.exit(1)
  }

  def main(args: Array[String]) = {
    parseArgs(args.toList)
    println("many-clients: %d items to %s using %d clients, kill rate %d%%, at %d msec/item".format(
      count, hostname, clientCount, killPercent, sleep))

    var threadList: List[Thread] = Nil
    val startTime = System.currentTimeMillis

    for (i <- 0 until clientCount) {
      val t = new Thread {
        override def run = {
          try {
            getStuff(count, hostname, 22133, "spam", useTransactions, killPercent)
          } catch {
            case e: Throwable =>
              e.printStackTrace()
          }
        }
      }
      threadList = t :: threadList
      t.start
    }
    val t = new Thread {
      override def run = {
        val socket = SocketChannel.open(new InetSocketAddress(hostname, 22133))
        try {
          put(sleep, socket, "spam", count)
        } catch {
          case e: Throwable =>
            e.printStackTrace()
        }
      }
    }
    threadList = t :: threadList
    t.start
    for (t <- threadList) {
      t.join
    }

    val duration = System.currentTimeMillis - startTime
    Console.println("Received %d items in %d msec.".format(got.get, duration))
  }
}
