/*
 * Copyright 2010 Twitter, Inc.
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

import java.net.InetSocketAddress
import com.twitter.conversions.time._
import com.twitter.naggati.test.TestCodec
import com.twitter.util.{Future, Time}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.ChannelGroup
import org.specs.Specification
import org.specs.mock.{Mockito}
import org.mockito.Matchers._  // to use matchers like anyInt()

class TextHandlerSpec extends Specification with Mockito {
  def wrap(s: String) = ChannelBuffers.wrappedBuffer(s.getBytes)

  "TextCodec" should {
    "get request" in {
      val codec = new TestCodec(TextCodec.read, TextCodec.write)

      codec(wrap("get foo\r\n")) mustEqual List(TextRequest("get", List("foo"), Nil))
      codec(wrap("get f")) mustEqual Nil
      codec(wrap("oo\r\n")) mustEqual List(TextRequest("get", List("foo"), Nil))

      codec(wrap("get foo 100")) mustEqual Nil
      codec(wrap("\n")) mustEqual List(TextRequest("get", List("foo", "100"), Nil))
    }

    "put request" in {
      val codec = new TestCodec(TextCodec.read, TextCodec.write)

      codec(wrap("put foo:\n")) mustEqual Nil
      codec(wrap("hello\n")) mustEqual Nil
      val stream = codec(wrap("\n"))
      stream.size mustEqual 1
      val r = stream(0).asInstanceOf[TextRequest]
      r.command mustEqual "put"
      r.args mustEqual List("foo")
      r.items.map { new String(_) } mustEqual List("hello")
    }

    "quit request" in {
      val codec = new TestCodec(TextCodec.read, TextCodec.write)
      codec(wrap("QUIT\r\n")) mustEqual List(TextRequest("quit", Nil, Nil))
    }

    "success response" in {
      val codec = new TestCodec(TextCodec.read, TextCodec.write)
      codec.send(CountResponse(23)) mustEqual List("+23\n")
    }

    "error response" in {
      val codec = new TestCodec(TextCodec.read, TextCodec.write)
      codec.send(ErrorResponse("Bad karma")) mustEqual List("-Bad karma\n")
    }

    "empty response" in {
      val codec = new TestCodec(TextCodec.read, TextCodec.write)
      codec.send(ItemResponse(None)) mustEqual List("*\n")
    }

    "item response" in {
      val codec = new TestCodec(TextCodec.read, TextCodec.write)
      codec.send(ItemResponse(Some("hello".getBytes))) mustEqual List(":hello\n")
    }
  }

  "TextHandler" should {
    val channel = mock[Channel]
    val channelGroup = mock[ChannelGroup]
    val queueCollection = mock[QueueCollection]
    val qitem = QItem(Time.now, None, "state shirt".getBytes, 23)

    "get request" in {
//      val function = capturingParam[Function[Option[QItem], Unit]]

      channel.getRemoteAddress() returns new InetSocketAddress(0)
      channelGroup.add(channel) returns true

      val textHandler = new TextHandler(channelGroup, queueCollection, 10, None)
      textHandler.handleUpstream(null, new UpstreamChannelStateEvent(channel, ChannelState.OPEN, true))

      "closes transactions" in {
        queueCollection.remove("test", None, true, false) returns Future.value(Some(qitem))

        textHandler.pendingTransactions.add("test", 100)
        textHandler.pendingTransactions.peek("test") mustEqual List(100)
        textHandler.handle(TextRequest("get", List("test"), Nil))
        textHandler.pendingTransactions.peek("test") mustEqual List(qitem.xid)

        there was one(queueCollection).remove("test", None, true, false)
        there was one(queueCollection).confirmRemove("test", 100)
      }

      "with timeout" in {
        Time.withCurrentTimeFrozen { time =>
          queueCollection.remove("test", Some(500.milliseconds.fromNow), true, false) returns Future.value(Some(qitem))

          textHandler.handle(TextRequest("get", List("test", "500"), Nil))

          there was one(queueCollection).remove("test", Some(500.milliseconds.fromNow), true, false)
        }
      }

      "empty queue" in {
        queueCollection.remove("test", None, true, false) returns Future.value(None)

        textHandler.handle(TextRequest("get", List("test"), Nil))

        there was one(queueCollection).remove("test", None, true, false)
        there was one(channel).write(ItemResponse(None))
      }

      "item ready" in {
        queueCollection.remove("test", None, true, false) returns Future.value(Some(qitem))

        textHandler.handle(TextRequest("get", List("test"), Nil))

        there was one(channel).write(ItemResponse(Some(qitem.data)))
      }
    }

    "put request" in {
      channel.getRemoteAddress() returns new InetSocketAddress(0)
      queueCollection.add("test", "hello".getBytes, None) returns true

      val textHandler = new TextHandler(channelGroup, queueCollection, 10, None)
      textHandler.handleUpstream(null, new UpstreamChannelStateEvent(channel, ChannelState.OPEN, true))
      textHandler.handle(TextRequest("put", List("test"), List("hello".getBytes)))

      there was one(channel).write(CountResponse(1))
    }
  }
}
