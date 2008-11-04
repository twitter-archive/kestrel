/** Copyright 2008 Twitter, Inc. */
package com.twitter.scarling.memcache

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
