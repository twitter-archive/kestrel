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

package net.lag.kestrel
package memcache

import org.apache.mina.core.buffer.IoBuffer
import org.apache.mina.core.filterchain.IoFilter
import org.apache.mina.core.session.{DummySession, IoSession}
import org.apache.mina.filter.codec._
import org.specs._


class MemCacheCodecSpec extends Specification {

  private val fakeSession = new DummySession

  private val fakeDecoderOutput = new ProtocolDecoderOutput {
    override def flush(nextFilter: IoFilter.NextFilter, s: IoSession) = {}
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
      val decoder = ASCIICodec.decoder

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
      val decoder = ASCIICodec.decoder

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

    "'quit' exits the session" in {
      val decoder = ASCIICodec.decoder

      decoder.decode(fakeSession, IoBuffer.wrap("QUIT\r\n".getBytes), fakeDecoderOutput)
      written.mkString(",") mustEqual "<Request: [QUIT]>"
    }
  }
}
