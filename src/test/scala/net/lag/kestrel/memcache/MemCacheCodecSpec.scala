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

import org.apache.mina.core.buffer.IoBuffer
import org.apache.mina.core.session.{DummySession, IoSession}
import org.apache.mina.filter.codec._
import org.specs._


object MemCacheCodecSpec extends Specification {

  private val fakeSession = new DummySession

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
      val decoder = memcache.Codec.decoder

      decoder.decode(fakeSession, IoBuffer.wrap("get foo\r\n".getBytes), fakeDecoderOutput)
      written mustEqual List(Request(List("GET", "foo"), None))
      written = Nil

      decoder.decode(fakeSession, IoBuffer.wrap("get f".getBytes), fakeDecoderOutput)
      written mustEqual Nil
      decoder.decode(fakeSession, IoBuffer.wrap("oo\r\n".getBytes), fakeDecoderOutput)
      written mustEqual List(Request(List("GET", "foo"), None))
      written = Nil

      decoder.decode(fakeSession, IoBuffer.wrap("g".getBytes), fakeDecoderOutput)
      written mustEqual Nil
      decoder.decode(fakeSession, IoBuffer.wrap("et foo\r".getBytes), fakeDecoderOutput)
      written mustEqual Nil
      decoder.decode(fakeSession, IoBuffer.wrap("\nget ".getBytes), fakeDecoderOutput)
      written mustEqual List(Request(List("GET", "foo"), None))
      decoder.decode(fakeSession, IoBuffer.wrap("bar\r\n".getBytes), fakeDecoderOutput)
      written mustEqual List(Request(List("GET", "bar"), None), Request(List("GET", "foo"), None))
    }

    "'set' request chunked various ways" in {
      val decoder = memcache.Codec.decoder

      decoder.decode(fakeSession, IoBuffer.wrap("set foo 0 0 5\r\nhello\r\n".getBytes), fakeDecoderOutput)
      written.mkString(",") mustEqual "<Request: [SET foo 0 0 5]: 68656c6c6f>"
      written = Nil

      decoder.decode(fakeSession, IoBuffer.wrap("set foo 0 0 5\r\n".getBytes), fakeDecoderOutput)
      written mustEqual Nil
      decoder.decode(fakeSession, IoBuffer.wrap("hello\r\n".getBytes), fakeDecoderOutput)
      written.mkString(",") mustEqual "<Request: [SET foo 0 0 5]: 68656c6c6f>"
      written = Nil

      decoder.decode(fakeSession, IoBuffer.wrap("set foo 0 0 5".getBytes), fakeDecoderOutput)
      written mustEqual Nil
      decoder.decode(fakeSession, IoBuffer.wrap("\r\nhell".getBytes), fakeDecoderOutput)
      written mustEqual Nil
      decoder.decode(fakeSession, IoBuffer.wrap("o\r\n".getBytes), fakeDecoderOutput)
      written.mkString(",") mustEqual "<Request: [SET foo 0 0 5]: 68656c6c6f>"
      written = Nil
    }
  }
}
