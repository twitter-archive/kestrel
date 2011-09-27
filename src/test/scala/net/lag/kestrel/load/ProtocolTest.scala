/*
 * Copyright 2009 Twitter, Inc.
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
object ProtocolTest {
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

  var clientCount = 100
  var totalItems = 10000
  var bytes = 1024
  var hostname = "localhost"
  var port = 22133
  var protocol = "memcache"

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
    Console.println("    -h HOSTNAME")
    Console.println("        use kestrel on HOSTNAME (default: %s)".format(hostname))
    Console.println("    -p PORT")
    Console.println("        use kestrel on PORT (default: %d)".format(port))
    Console.println("    -t PROTOCOL")
    Console.println("        use kestrel on PROTOCOL (default: %d)".format(port))
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
    case "-h" :: x :: xs =>
      hostname = x
      parseArgs(xs)
    case "-p" :: x :: xs =>
      port = x.toInt
      parseArgs(xs)
    case "-t" :: x :: xs =>
      protocol = x
      parseArgs(xs)
    case _ =>
      usage()
      System.exit(1)
  }

  def main(args: Array[String]) = {
    parseArgs(args.toList)
    println("Put %d items of %d bytes to %s:%d using %d clients.".format(totalItems, bytes, hostname, port, clientCount))

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

    val protocolIf2 = Client.create(protocol, hostname, port)
    protocolIf2.flush("spam")
    protocolIf2.release()

    var threadList: List[Thread] = Nil
    val startTime = System.currentTimeMillis

    for (i <- 0 until clientCount) {
      val t = new Thread {
        override def run = {
          val qName = "spam" // + (i % totalQueues)
          val protocolIf = Client.create(protocol, hostname, port)
          protocolIf.put(qName, totalItems / clientCount, rawData.toString)
          protocolIf.release()
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
  }
}



// vim: set ts=4 sw=4 et:
