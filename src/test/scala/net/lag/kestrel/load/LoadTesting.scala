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
}
