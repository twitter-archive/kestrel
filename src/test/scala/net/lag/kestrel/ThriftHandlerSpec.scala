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
import com.twitter.util.{Future, Promise, Time, MockTimer}
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
    val timer = new MockTimer()

    val item1 = "forty second songs".getBytes
    val item2 = "novox the robot".getBytes
    val item3 = "danger bus".getBytes

    doBefore {
      ThriftPendingReads.reset()
    }

    expect {
      one(connection).remoteAddress willReturn address
    }

    val thriftHandler = new ThriftHandler(connection, queueCollection, 10, timer)

    "put" in {
      "one" in {
        Time.withCurrentTimeFrozen { mutator =>
          expect {
            one(queueCollection).add("test", item1, None, Time.now) willReturn true
          }

          thriftHandler.put("test", List(ByteBuffer.wrap(item1)), 0)() mustEqual 1
        }
      }

      "two" in {
        Time.withCurrentTimeFrozen { mutator =>
          expect {
            one(queueCollection).add("test", item1, None, Time.now) willReturn true
            one(queueCollection).add("test", item2, None, Time.now) willReturn true
          }

          thriftHandler.put("test", List(ByteBuffer.wrap(item1), ByteBuffer.wrap(item2)), 0)() mustEqual 2
        }
      }

      "three, with only one accepted" in {
        Time.withCurrentTimeFrozen { mutator =>
          expect {
            one(queueCollection).add("test", item1, None, Time.now) willReturn true
            one(queueCollection).add("test", item2, None, Time.now) willReturn false
          }

          thriftHandler.put("test", List(
            ByteBuffer.wrap(item1),
            ByteBuffer.wrap(item2),
            ByteBuffer.wrap(item3)
          ), 0)() mustEqual 1
        }
      }

      "with timeout" in {
        Time.withCurrentTimeFrozen { mutator =>
          expect {
            one(queueCollection).add("test", item1, Some(5.seconds.fromNow), Time.now) willReturn true
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

        thriftHandler.get("test", 1, 0, 0)() mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 0L))
      }

      "one, with timeout" in {
        Time.withCurrentTimeFrozen { mutator =>
          val qitem = QItem(Time.now, None, item1, 0)

          expect {
            one(queueCollection).remove("test", Some(1.second.fromNow), false, false) willReturn Future(Some(qitem))
          }

          thriftHandler.get("test", 1, 1000, 0)() mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 0L))
        }
      }

      "one, reliably" in {
        val qitem = QItem(Time.now, None, item1, 1)

        expect {
          one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem))
        }

        thriftHandler.get("test", 1, 0, 500)() mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 1L))
      }

      "multiple" in {
        val qitem1 = QItem(Time.now, None, item1, 1)
        val qitem2 = QItem(Time.now, None, item2, 2)

        expect {
          one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem1))
          one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem2))
          one(queueCollection).remove("test", None, true, false) willReturn Future(None)
        }

        thriftHandler.get("test", 5, 0, 500)() mustEqual List(
          thrift.Item(ByteBuffer.wrap(item1), 1L),
          thrift.Item(ByteBuffer.wrap(item2), 2L)
        )
      }

      "multiple queues" in {
        val qitem1 = QItem(Time.now, None, item1, 1)
        val qitem2 = QItem(Time.now, None, item2, 1)

        expect {
          one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem1))
          one(queueCollection).remove("spam", None, true, false) willReturn Future(Some(qitem2))
        }

        thriftHandler.get("test", 1, 0, 500)() mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 1L))
        thriftHandler.get("spam", 1, 0, 500)() mustEqual List(thrift.Item(ByteBuffer.wrap(item2), 2L))
      }

      "too many open transations" in {
        val qitems = (1 to 10).map { i => QItem(Time.now, None, item1, i) }
        expect {
          qitems.foreach { qitem =>
            one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem))
          }
        }

        thriftHandler.get("test", 10, 0, 500)() mustEqual (1 to 10).map { i =>
          thrift.Item(ByteBuffer.wrap(item1), i.toLong)
        }

        thriftHandler.get("test", 10, 0, 500)() mustEqual List()
      }
    }

    "confirm" in {
      expect {
        one(queueCollection).confirmRemove("test", 2)
        one(queueCollection).confirmRemove("test", 3)
      }

      thriftHandler.handler.addPendingRead("test", 2)
      thriftHandler.handler.addPendingRead("test", 3)
      thriftHandler.confirm("test", Set(1L, 2L))
    }

    "abort" in {
      expect {
        one(queueCollection).unremove("test", 2)
        one(queueCollection).unremove("test", 3)
      }

      thriftHandler.handler.addPendingRead("test", 2)
      thriftHandler.handler.addPendingRead("test", 3)
      thriftHandler.abort("test", Set(1L, 2L))
    }

    "auto-abort" in {
      "one" in {
        Time.withCurrentTimeFrozen { time =>
          val qitem = QItem(Time.now, None, item1, 1)

          expect {
            one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem))
            one(queueCollection).unremove("test", 1)
          }

          thriftHandler.get("test", 1, 0, 500)() mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 1L))

          time.advance(501.milliseconds)
          timer.tick()
        }
      }

      "multiple" in {
        Time.withCurrentTimeFrozen { time =>
          val qitem1 = QItem(Time.now, None, item1, 1)
          val qitem2 = QItem(Time.now, None, item2, 2)

          expect {
            one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem1))
            one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem2))
            one(queueCollection).remove("test", None, true, false) willReturn Future(None)
            one(queueCollection).unremove("test", 1)
            one(queueCollection).unremove("test", 2)
          }

          thriftHandler.get("test", 5, 0, 500)() mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 1L),
                                                                thrift.Item(ByteBuffer.wrap(item2), 2L))

          time.advance(501.milliseconds)
          timer.tick()
        }
      }

      "cleared by manual abort" in {
        Time.withCurrentTimeFrozen { time =>
          val qitem = QItem(Time.now, None, item1, 1)

          expect {
            one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem))
            one(queueCollection).unremove("test", 1)
          }

          thriftHandler.get("test", 1, 0, 500)() mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 1L))
          thriftHandler.abort("test", Set(1L))() mustEqual 1

          timer.tasks.size mustEqual 0
        }
      }

      "cleared by confirm" in {
        Time.withCurrentTimeFrozen { time =>
          val qitem = QItem(Time.now, None, item1, 1)

          expect {
            one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem))
            one(queueCollection).confirmRemove("test", 1)
          }

          thriftHandler.get("test", 1, 0, 500)() mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 1L))
          thriftHandler.confirm("test", Set(1L))() mustEqual 1

          timer.tasks.size mustEqual 0
        }
      }

      "multiple, some confirmed" in {
        Time.withCurrentTimeFrozen { time =>
          val qitem1 = QItem(Time.now, None, item1, 1)
          val qitem2 = QItem(Time.now, None, item2, 2)
          val qitem3 = QItem(Time.now, None, item3, 3)

          expect {
            one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem1))
            one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem2))
            one(queueCollection).remove("test", None, true, false) willReturn Future(Some(qitem3))
            one(queueCollection).remove("test", None, true, false) willReturn Future(None)
            one(queueCollection).confirmRemove("test", 1)
            one(queueCollection).unremove("test", 2)
            one(queueCollection).confirmRemove("test", 3)
          }

          thriftHandler.get("test", 5, 0, 500)()
          thriftHandler.confirm("test", Set(1L, 3L))() mustEqual 2

          timer.tasks.size mustEqual 1

          time.advance(501.milliseconds)
          timer.tick()

          timer.tasks.size mustEqual 0
        }
      }
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

    "flush_queue" in {
      expect {
        one(queueCollection).flush("test")
      }

      thriftHandler.flushQueue("test")
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

    "flush_all_queues" in {
      expect {
        one(queueCollection).queueNames willReturn List("test", "spam")
        one(queueCollection).flush("test")
        one(queueCollection).flush("spam")
      }

      thriftHandler.flushAllQueues()
    }
  }
}
