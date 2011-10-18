/*
 * Copyright 2011 Twitter, Inc.
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

import com.twitter.conversions.time._
import com.twitter.finagle.ClientConnection
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.util.{Future, Promise, Time}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}

class ThriftHandlerSpec extends Specification with JMocker with ClassMocker {
  def wrap(s: String) = ChannelBuffers.wrappedBuffer(s.getBytes)

  "ThriftHandler" should {
    val queueCollection = mock[QueueCollection]
    val connection = mock[ClientConnection]
    val address = mock[InetSocketAddress]

    val item1 = "forty second songs".getBytes
    val item2 = "novox the robot".getBytes
    val item3 = "danger bus".getBytes

    expect {
      one(connection).remoteAddress willReturn address
    }

    val thriftHandler = new ThriftHandler(connection, queueCollection, 10)

    "put" in {
      "one" in {
        expect {
          one(queueCollection).add("test", item1, None) willReturn true
        }

        thriftHandler.put("test", List(ByteBuffer.wrap(item1)), 0)() mustEqual 1
      }

      "two" in {
        expect {
          one(queueCollection).add("test", item1, None) willReturn true
          one(queueCollection).add("test", item2, None) willReturn true
        }

        thriftHandler.put("test", List(ByteBuffer.wrap(item1), ByteBuffer.wrap(item2)), 0)() mustEqual 2
      }

      "three, with only one accepted" in {
        expect {
          one(queueCollection).add("test", item1, None) willReturn true
          one(queueCollection).add("test", item2, None) willReturn false
        }

        thriftHandler.put("test", List(
          ByteBuffer.wrap(item1),
          ByteBuffer.wrap(item2),
          ByteBuffer.wrap(item3)
        ), 0)() mustEqual 1
      }

      "with timeout" in {
        Time.withCurrentTimeFrozen { mutator =>
          expect {
            one(queueCollection).add("test", item1, Some(5.seconds.fromNow)) willReturn true
          }

          thriftHandler.put("test", List(ByteBuffer.wrap(item1)), 5000)() mustEqual 1
        }
      }
    }

    "get" in {
      "one, no timeout" in {
        val qitem = QItem(Time.now, None, item1, 0)

        expect {
          one(queueCollection).remove("test", None, false, false) willReturn Future(Some(qitem))
        }

        thriftHandler.get("test", 1, 0, true)() mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 0))
      }

      "one, with timeout" in {
        Time.withCurrentTimeFrozen { mutator =>
          val qitem = QItem(Time.now, None, item1, 0)

          expect {
            one(queueCollection).remove("test", Some(1.second.fromNow), false, false) willReturn Future(Some(qitem))
          }

          thriftHandler.get("test", 1, 1000, true)() mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 0))
        }
      }

      "one, reliably" in {
        val qitem = QItem(Time.now, None, item1, 1)

        expect {
          one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem))
        }

        thriftHandler.get("test", 1, 0, false)() mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 1))
      }

      "multiple" in {
        val qitem1 = QItem(Time.now, None, item1, 0)
        val qitem2 = QItem(Time.now, None, item2, 0)

        expect {
          one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem1))
          one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem2))
          one(queueCollection).remove("test", None, true, false) willReturn Future(None)
        }

        thriftHandler.get("test", 5, 0, false)() mustEqual List(
          thrift.Item(ByteBuffer.wrap(item1), 0),
          thrift.Item(ByteBuffer.wrap(item2), 0)
        )
      }
    }

    "confirm" in {
      expect {
        one(queueCollection).confirmRemove("test", 2)
        one(queueCollection).confirmRemove("test", 3)
      }

      thriftHandler.handler.pendingReads.add("test", 2)
      thriftHandler.handler.pendingReads.add("test", 3)
      thriftHandler.confirm("test", Set(2, 3))
    }

    "abort" in {
      expect {
        one(queueCollection).unremove("test", 2)
        one(queueCollection).unremove("test", 3)
      }

      thriftHandler.handler.pendingReads.add("test", 2)
      thriftHandler.handler.pendingReads.add("test", 3)
      thriftHandler.abort("test", Set(2, 3))
    }

    "peek" in {
      val qitem1 = QItem(Time.now, None, item1, 0)

      expect {
        one(queueCollection).remove("test", None, false, true) willReturn Future(Some(qitem1))
        one(queueCollection).stats("test") willReturn List(
          ("items", "10"),
          ("bytes", "10240"),
          ("logsize", "29999"),
          ("age", "500"),
          ("waiters", "2"),
          ("open_transactions", "1")
        ).toArray
      }

      val qinfo = new thrift.QueueInfo(Some(ByteBuffer.wrap(item1)), 10, 10240, 29999, 500, 2, 1)
      thriftHandler.peek("test")() mustEqual qinfo
    }

    "flush" in {
      expect {
        one(queueCollection).flush("test")
      }

      thriftHandler.flush("test")
    }

    "delete_queue" in {
      expect {
        one(queueCollection).delete("test")
      }

      thriftHandler.deleteQueue("test")
    }

    "get_version" in {
      val runtime = RuntimeEnvironment(this, Array())
      Kestrel.runtime = runtime
      thriftHandler.getVersion()() must haveClass[String]
    }

    "flush_all" in {
      expect {
        one(queueCollection).queueNames willReturn List("test", "spam")
        one(queueCollection).flush("test")
        one(queueCollection).flush("spam")
      }

      thriftHandler.flushAll()
    }

    "flush_all_expired" in {
      expect {
        one(queueCollection).flushAllExpired() willReturn 2
      }

      thriftHandler.flushAllExpired()() mustEqual 2
    }
  }
}
