/*
 * Copyright (c) 2008 Robey Pointer <robeypointer@lag.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package net.lag.kestrel.load

import java.net._
import java.nio._
import java.nio.channels._
import java.util.concurrent.atomic._
import net.lag.extensions._


/**
 * Have one producer trickle in pop lyrics at a steady but slow rate, while
 * many clients clamor for each one. This is similar to how queues operate in
 * some typical production environments.
 */
object ManyClients {
  private val SLEEP = 100
  private val COUNT = 100

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


  def put(socket: SocketChannel, queueName: String, n: Int) = {
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
      Thread.sleep(SLEEP)
    }
  }

  def getStuff(socket: SocketChannel, queueName: String) = {
    val req = ByteBuffer.wrap(("get " + queueName + "/t=1000\r\n").getBytes)
    val expectEnd = ByteBuffer.wrap("END\r\n".getBytes)
    val expectLyric = ByteBuffer.wrap(("VALUE " + queueName + " 0 " + LYRIC.length + "\r\n" + LYRIC + "\r\n").getBytes)
    val buffer = ByteBuffer.allocate(expectLyric.capacity)
    expectLyric.rewind

    while (got.get < COUNT) {
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
        // ok. :(
      } else {
        buffer.position(oldpos)
        buffer.limit(buffer.capacity)
        while (buffer.position < expectLyric.limit) {
          socket.read(buffer)
        }
        buffer.rewind
        expectLyric.rewind
        if (buffer != expectLyric) {
          throw new Exception("Unexpected response!")
        }
        println("" + got.incrementAndGet)
      }
    }
  }

  def main(args: Array[String]) = {
    if (args.length < 1) {
      Console.println("usage: many-clients <N>")
      Console.println("    spin up N clients and have them do timeout reads on a queue while a")
      Console.println("    single producer trickles out.")
      System.exit(1)
    }

    val clientCount = args(0).toInt

    var threadList: List[Thread] = Nil
    val startTime = System.currentTimeMillis

    for (i <- 0 until clientCount) {
      val t = new Thread {
        override def run = {
          val socket = SocketChannel.open(new InetSocketAddress("localhost", 22133))
          getStuff(socket, "spam")
        }
      }
      threadList = t :: threadList
      t.start
    }
    val t = new Thread {
      override def run = {
        val socket = SocketChannel.open(new InetSocketAddress("localhost", 22133))
        put(socket, "spam", COUNT)
      }
    }
    threadList = t :: threadList
    t.start
    for (t <- threadList) {
      t.join
    }

    val duration = System.currentTimeMillis - startTime
    Console.println("Finished in %d msec.".format(duration))
  }
}
