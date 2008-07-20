package com.twitter.scarling.memcache

import org.apache.mina.common.{ByteBuffer, IoSession}
import org.apache.mina.common.support.{BaseIoSession}
import org.apache.mina.filter.codec._
import sorg.testing._


object MemCacheCodecTests extends Tests {

    override def testName = "MemCacheCodecTests"

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

    override def setUp = {
        written = Nil
    }


    test("'get' request chunked various ways") {
        val decoder = new Decoder

        decoder.decode(fakeSession, ByteBuffer.wrap("get foo\r\n".getBytes), fakeDecoderOutput)
        expect(List(Request(List("GET", "foo"), None))) { written }
        written = Nil

        decoder.decode(fakeSession, ByteBuffer.wrap("get f".getBytes), fakeDecoderOutput)
        expect(List()) { written }
        decoder.decode(fakeSession, ByteBuffer.wrap("oo\r\n".getBytes), fakeDecoderOutput)
        expect(List(Request(List("GET", "foo"), None))) { written }
        written = Nil

        decoder.decode(fakeSession, ByteBuffer.wrap("g".getBytes), fakeDecoderOutput)
        expect(List()) { written }
        decoder.decode(fakeSession, ByteBuffer.wrap("et foo\r".getBytes), fakeDecoderOutput)
        expect(List()) { written }
        decoder.decode(fakeSession, ByteBuffer.wrap("\nget ".getBytes), fakeDecoderOutput)
        expect(List(Request(List("GET", "foo"), None))) { written }
        decoder.decode(fakeSession, ByteBuffer.wrap("bar\r\n".getBytes), fakeDecoderOutput)
        expect(List(Request(List("GET", "bar"), None), Request(List("GET", "foo"), None))) { written }
    }

    test("'set' request chunked various ways") {
        val decoder = new Decoder

        decoder.decode(fakeSession, ByteBuffer.wrap("set foo 0 0 5\r\nhello\r\n".getBytes), fakeDecoderOutput)
        expect("<Request: [SET foo 0 0 5]: 68656c6c6f>") { written.mkString(",") }
        written = Nil

        decoder.decode(fakeSession, ByteBuffer.wrap("set foo 0 0 5\r\n".getBytes), fakeDecoderOutput)
        expect(List()) { written }
        decoder.decode(fakeSession, ByteBuffer.wrap("hello\r\n".getBytes), fakeDecoderOutput)
        expect("<Request: [SET foo 0 0 5]: 68656c6c6f>") { written.mkString(",") }
        written = Nil

        decoder.decode(fakeSession, ByteBuffer.wrap("set foo 0 0 5".getBytes), fakeDecoderOutput)
        expect(List()) { written }
        decoder.decode(fakeSession, ByteBuffer.wrap("\r\nhell".getBytes), fakeDecoderOutput)
        expect(List()) { written }
        decoder.decode(fakeSession, ByteBuffer.wrap("o\r\n".getBytes), fakeDecoderOutput)
        expect("<Request: [SET foo 0 0 5]: 68656c6c6f>") { written.mkString(",") }
        written = Nil
    }
}
