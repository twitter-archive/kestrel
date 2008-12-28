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

package net.lag.kestrel.memcache

import scala.collection.mutable
import org.apache.mina.core.buffer.IoBuffer
import org.apache.mina.core.session.{IdleStatus, IoSession}
import org.apache.mina.filter.codec._
import net.lag.extensions._
import net.lag.naggati.{Decoder, End, ProtocolError, Step}
import net.lag.naggati.Steps._


case class Request(line: List[String], data: Option[Array[Byte]]) {
  override def toString = {
    "<Request: " + line.mkString("[", " ", "]") + (data match {
      case None => ""
      case Some(x) => ": " + x.hexlify
    }) + ">"
  }
}

case class Response(data: IoBuffer)


object Codec {
  private val KNOWN_COMMANDS = List("GET", "SET", "STATS", "SHUTDOWN", "RELOAD")
  private val DATA_COMMANDS = List("SET")

  val encoder = new ProtocolEncoder {
    def encode(session: IoSession, message: AnyRef, out: ProtocolEncoderOutput) = {
      val buffer = message.asInstanceOf[Response].data
      KestrelStats.bytesWritten.incr(buffer.remaining)
      out.write(buffer)
    }

    def dispose(session: IoSession): Unit = {
      // nothing.
    }
  }

  val decoder = new Decoder(readLine(true, "ISO-8859-1") { line =>
    val segments = line.split(" ")
    segments(0) = segments(0).toUpperCase

    val command = segments(0)
    if (! KNOWN_COMMANDS.contains(command)) {
      throw new ProtocolError("Invalid command: " + command)
    }

    if (DATA_COMMANDS.contains(command)) {
      if (segments.length < 5) {
        throw new ProtocolError("Malformed request line")
      }
      val dataBytes = segments(4).toInt
      readBytes(dataBytes + 2) {
        // final 2 bytes are just "\r\n" mandated by protocol.
        val bytes = new Array[Byte](dataBytes)
        state.buffer.get(bytes)
        state.buffer.position(state.buffer.position + 2)
        state.out.write(Request(segments.toList, Some(bytes)))
        End
      }
    } else {
      state.out.write(Request(segments.toList, None))
      End
    }
  })
}
