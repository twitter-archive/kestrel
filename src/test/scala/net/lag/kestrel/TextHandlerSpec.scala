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
import com.twitter.util.{Future, Promise, Time}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.ChannelGroup
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}

class TextHandlerSpec extends Specification with JMocker with ClassMocker {
  def wrap(s: String) = ChannelBuffers.wrappedBuffer(s.getBytes)

  "TextCodec" should {
    "get request" in {
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)

      codec(wrap("get foo\r\n")) mustEqual List(TextRequest("get", List("foo"), Nil))
      codec(wrap("get f")) mustEqual Nil
      codec(wrap("oo\r\n")) mustEqual List(TextRequest("get", List("foo"), Nil))

      codec(wrap("get foo 100")) mustEqual Nil
      codec(wrap("\n")) mustEqual List(TextRequest("get", List("foo", "100"), Nil))
    }

    "put request" in {
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)

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
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)
      codec(wrap("QUIT\r\n")) mustEqual List(TextRequest("quit", Nil, Nil))
    }

    "success response" in {
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)
      codec.send(CountResponse(23)) mustEqual List("+23\n")
    }

    "error response" in {
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)
      codec.send(ErrorResponse("Bad karma")) mustEqual List("-Bad karma\n")
    }

    "empty response" in {
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)
      codec.send(ItemResponse(None)) mustEqual List("*\n")
    }

    "item response" in {
      val (codec, counter) = TestCodec(TextCodec.read, TextCodec.write)
      codec.send(ItemResponse(Some("hello".getBytes))) mustEqual List(":hello\n")
    }
  }

  "TextHandler" should {
    val channel = mock[Channel]
    val channelGroup = mock[ChannelGroup]
    val queueCollection = mock[QueueCollection]
    val qitem = QItem(Time.now, None, "state shirt".getBytes, 23)

    "get request" in {
      expect {
        one(channel).getRemoteAddress() willReturn new InetSocketAddress(0)
        one(channelGroup).add(channel) willReturn true
      }

      val textHandler = new TextHandler(channelGroup, queueCollection, 10, None)
      textHandler.handleUpstream(null, new UpstreamChannelStateEvent(channel, ChannelState.OPEN, true))

      "closes transactions" in {
        expect {
          one(queueCollection).remove("test", None, true, false) willReturn Future.value(Some(qitem))
          one(queueCollection).confirmRemove("test", 100)
          one(channel).write(ItemResponse(Some(qitem.data)))
        }

        textHandler.pendingTransactions.add("test", 100)
        textHandler.pendingTransactions.peek("test") mustEqual List(100)
        textHandler.handle(TextRequest("get", List("test"), Nil))
        textHandler.pendingTransactions.peek("test") mustEqual List(qitem.xid)
      }

      "with timeout" in {
        "value ready immediately" in {
          Time.withCurrentTimeFrozen { time =>
            expect {
              one(queueCollection).remove("test", Some(500.milliseconds.fromNow), true, false) willReturn Future.value(Some(qitem))
              one(channel).write(ItemResponse(Some(qitem.data)))
            }

            textHandler.handle(TextRequest("get", List("test", "500"), Nil))
          }
        }

        "value ready eventually" in {
          Time.withCurrentTimeFrozen { time =>
            val promise = new Promise[Option[QItem]]

            expect {
              one(queueCollection).remove("test", Some(500.milliseconds.fromNow), true, false) willReturn promise
            }

            textHandler.handle(TextRequest("get", List("test", "500"), Nil))

            expect {
              one(channel).write(ItemResponse(Some(qitem.data)))
            }

            promise.setValue(Some(qitem))
          }
        }

        "timed out" in {
          Time.withCurrentTimeFrozen { time =>
            val promise = new Promise[Option[QItem]]

            expect {
              one(queueCollection).remove("test", Some(500.milliseconds.fromNow), true, false) willReturn promise
            }

            textHandler.handle(TextRequest("get", List("test", "500"), Nil))

            expect {
              one(channel).write(ItemResponse(None))
            }

            promise.setValue(None)
          }
        }
      }

      "empty queue" in {
        expect {
          one(queueCollection).remove("test", None, true, false) willReturn Future.value(None)
          one(channel).write(ItemResponse(None))
        }

        textHandler.handle(TextRequest("get", List("test"), Nil))
      }

      "item ready" in {
        expect {
          one(queueCollection).remove("test", None, true, false) willReturn Future.value(Some(qitem))
          one(channel).write(ItemResponse(Some(qitem.data)))
        }

        textHandler.handle(TextRequest("get", List("test"), Nil))
      }
    }

    "put request" in {
      expect {
        one(channel).getRemoteAddress() willReturn new InetSocketAddress(0)
        one(channelGroup).add(channel) willReturn true
        one(queueCollection).add("test", "hello".getBytes, None) willReturn true
        one(channel).write(CountResponse(1))
      }

      val textHandler = new TextHandler(channelGroup, queueCollection, 10, None)
      textHandler.handleUpstream(null, new UpstreamChannelStateEvent(channel, ChannelState.OPEN, true))
      textHandler.handle(TextRequest("put", List("test"), List("hello".getBytes)))
    }

    // FIXME: peek, monitor, confirm, flush, quit, shutdown, unknown
  }
}
