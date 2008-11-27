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

package net.lag.scarling.memcache

import org.apache.mina.common.{ByteBuffer, IoSession}
import org.apache.mina.common.support.{BaseIoSession}
import org.apache.mina.filter.codec._
import org.specs._


object MemCacheCodecSpec extends Specification {

  private val fakeSession = new BaseIoSession {
    override def updateTrafficMask = {}
    override def getServiceAddress = null
    override def getLocalAddress = null
    override def getRemoteAddress = null
    override def getTransportType = null
    override def getFilterChain = null
    override def getConfig = null
    override def getHandler = null
    override def getServiceConfig = null
    override def getService = null
  }

  private val fakeDecoderOutput = new ProtocolDecoderOutput {
    override def flush = {}
    override def write(obj: AnyRef) = {
      written = obj :: written
    }
  }

  private var written: List[AnyRef] = Nil


  "Memcache Decoder" should {
    doBefore {
      written = Nil
    }


    "'get' request chunked various ways" in {
      val decoder = new Decoder

      decoder.decode(fakeSession, ByteBuffer.wrap("get foo\r\n".getBytes), fakeDecoderOutput)
      written mustEqual List(Request(List("GET", "foo"), None))
      written = Nil

      decoder.decode(fakeSession, ByteBuffer.wrap("get f".getBytes), fakeDecoderOutput)
      written mustEqual Nil
      decoder.decode(fakeSession, ByteBuffer.wrap("oo\r\n".getBytes), fakeDecoderOutput)
      written mustEqual List(Request(List("GET", "foo"), None))
      written = Nil

      decoder.decode(fakeSession, ByteBuffer.wrap("g".getBytes), fakeDecoderOutput)
      written mustEqual Nil
      decoder.decode(fakeSession, ByteBuffer.wrap("et foo\r".getBytes), fakeDecoderOutput)
      written mustEqual Nil
      decoder.decode(fakeSession, ByteBuffer.wrap("\nget ".getBytes), fakeDecoderOutput)
      written mustEqual List(Request(List("GET", "foo"), None))
      decoder.decode(fakeSession, ByteBuffer.wrap("bar\r\n".getBytes), fakeDecoderOutput)
      written mustEqual List(Request(List("GET", "bar"), None), Request(List("GET", "foo"), None))
    }

    "'set' request chunked various ways" in {
      val decoder = new Decoder

      decoder.decode(fakeSession, ByteBuffer.wrap("set foo 0 0 5\r\nhello\r\n".getBytes), fakeDecoderOutput)
      written.mkString(",") mustEqual "<Request: [SET foo 0 0 5]: 68656c6c6f>"
      written = Nil

      decoder.decode(fakeSession, ByteBuffer.wrap("set foo 0 0 5\r\n".getBytes), fakeDecoderOutput)
      written mustEqual Nil
      decoder.decode(fakeSession, ByteBuffer.wrap("hello\r\n".getBytes), fakeDecoderOutput)
      written.mkString(",") mustEqual "<Request: [SET foo 0 0 5]: 68656c6c6f>"
      written = Nil

      decoder.decode(fakeSession, ByteBuffer.wrap("set foo 0 0 5".getBytes), fakeDecoderOutput)
      written mustEqual Nil
      decoder.decode(fakeSession, ByteBuffer.wrap("\r\nhell".getBytes), fakeDecoderOutput)
      written mustEqual Nil
      decoder.decode(fakeSession, ByteBuffer.wrap("o\r\n".getBytes), fakeDecoderOutput)
      written.mkString(",") mustEqual "<Request: [SET foo 0 0 5]: 68656c6c6f>"
      written = Nil
    }
  }
}
