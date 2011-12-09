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

package net.lag.kestrel
package load

import java.net._
import java.nio._
import java.nio.channels._
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import scala.collection.mutable
import com.twitter.conversions.string._

class Expecting(buffers: ByteBuffer*) {
  val sortedBuffers = buffers.sortBy(_.limit).toList
  val limit = sortedBuffers.last.limit
  val buffer = ByteBuffer.allocate(limit)

  def apply(socket: SocketChannel): ByteBuffer = {
    buffer.rewind()
    buffer.limit(0)
    check(socket, sortedBuffers)
  }

  @tailrec
  private final def check(socket: SocketChannel, remaining: List[ByteBuffer]): ByteBuffer = {
    if (remaining == Nil) {
      throw new Exception("Expected one of " + sortedBuffers.map { b => bufferToString(b) } + "; got " + bufferToString(buffer) + "!")
    }
    val expected = remaining.head
    buffer.position(buffer.limit)
    buffer.limit(expected.limit)
    while (buffer.position < buffer.limit) {
      socket.read(buffer)
    }
    buffer.rewind()
    expected.rewind()
    if (buffer == expected) {
      expected
    } else {
      check(socket, remaining.tail)
    }
  }

  private final def bufferToString(b: ByteBuffer) = {
    val bytes = new Array[Byte](b.limit)
    b.rewind()
    b.get(bytes)
    new String(bytes, "ISO-8859-1")
  }
}

// useful tools for building various load/performance tests.
trait LoadTesting {
  final def send(socket: SocketChannel, data: ByteBuffer) = {
    val startTime = System.nanoTime
    data.rewind()
    while (data.position < data.limit) {
      socket.write(data)
    }
    System.nanoTime - startTime
  }

  final def receive(socket: SocketChannel, data: ByteBuffer) = {
    data.rewind()
    while (data.position < data.limit) {
      socket.read(data)
    }
    data.rewind()
    data
  }

  final def expect(socket: SocketChannel, data: ByteBuffer) {
    val buffer = ByteBuffer.allocate(data.capacity)
    receive(socket, buffer)
    data.rewind()
    if (buffer != data) {
      throw new Exception("Unexpected response!")
    }
  }

  val failedConnects = new AtomicInteger(0)

  final def tryHard[A](f: => A): A = {
    try {
      f
    } catch {
      case e: java.io.IOException =>
        failedConnects.incrementAndGet()
        tryHard(f)
    }
  }

  def monitorQueue(hostname: String, queueName: String) {
    val t = new Thread("monitor-queue") {
      override def run() {
        val client = new TestClient(hostname, 22133)
        client.connect()
        while (true) {
          val stats = client.stats()
          val items = stats.getOrElse("queue_" + queueName + "_items", "0").toInt
          val bytes = stats.getOrElse("queue_" + queueName + "_bytes", "0").toInt
          val memItems = stats.getOrElse("queue_" + queueName + "_mem_items", "0").toInt
          val memBytes = stats.getOrElse("queue_" + queueName + "_mem_bytes", "0").toInt
          val journalBytes = stats.getOrElse("queue_" + queueName + "_logsize", "0").toInt
          println("%s: items=%d bytes=%d mem_items=%d mem_bytes=%d journal_bytes=%d".format(
            queueName, items, bytes, memItems, memBytes, journalBytes
          ))
          Thread.sleep(1000)
        }
      }
    }
    t.setDaemon(true)
    t.start()
  }
}
