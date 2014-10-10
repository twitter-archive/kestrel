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
import com.twitter.util.{Await, Future, Promise, Time, TimeControl, MockTimer}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.specs.SpecificationWithJUnit
import org.specs.mock.{ClassMocker, JMocker}
import net.lag.kestrel.thrift.{Status => TStatus}

class ThriftHandlerSpec extends SpecificationWithJUnit with JMocker with ClassMocker {
  def wrap(s: String) = ChannelBuffers.wrappedBuffer(s.getBytes)

  type ClientDesc = Option[() => String]

  "ThriftHandler" should {
    val queueCollection = mock[QueueCollection]
    val connection = mock[ClientConnection]
    val address = new InetSocketAddress(10000)
    val timer = new MockTimer()

    val item1 = "forty second songs".getBytes
    val item2 = "novox the robot".getBytes
    val item3 = "danger bus".getBytes

    doBefore {
      ThriftPendingReads.reset()
    }

    def withThriftHandler(f: (ThriftHandler) => Unit) {
      expect {
        one(connection).remoteAddress willReturn address
      }

      val thriftHandler = new ThriftHandler(connection, queueCollection, 10, timer)

      f(thriftHandler)
    }

    def withFrozenThriftHandler(f: (ThriftHandler, TimeControl) => Unit) {
      expect {
        one(connection).remoteAddress willReturn address
      }

      val thriftHandler = new ThriftHandler(connection, queueCollection, 10, timer)

      Time.withCurrentTimeFrozen { mutator =>
        f(thriftHandler, mutator)
      }
    }

    def withServerStatusThriftHandler(f: (ThriftHandler, ServerStatus) => Unit) {
      val serverStatus = mock[ServerStatus]

      expect {
        one(connection).remoteAddress willReturn address
      }

      val thriftHandler = new ThriftHandler(connection, queueCollection, 10, timer, Some(serverStatus))

      f(thriftHandler, serverStatus)
    }

    "put" in {
      "one" in {
        withFrozenThriftHandler { (thriftHandler, mutator) =>
          expect {
            one(queueCollection).add(equal("test"), equal(item1), equal(None), equal(Time.now), any[ClientDesc]) willReturn true
          }

          Await.result(thriftHandler.put("test", List(ByteBuffer.wrap(item1)), 0)) mustEqual 1
        }
      }

      "two" in {
        withFrozenThriftHandler { (thriftHandler, mutator) =>
          expect {
            one(queueCollection).add(equal("test"), equal(item1), equal(None), equal(Time.now), any[ClientDesc]) willReturn true
            one(queueCollection).add(equal("test"), equal(item2), equal(None), equal(Time.now), any[ClientDesc]) willReturn true
          }

          Await.result(thriftHandler.put("test", List(ByteBuffer.wrap(item1), ByteBuffer.wrap(item2)), 0)) mustEqual 2
        }
      }

      "three, with only one accepted" in {
        withFrozenThriftHandler { (thriftHandler, mutator) =>
          expect {
            one(queueCollection).add(equal("test"), equal(item1), equal(None), equal(Time.now), any[ClientDesc]) willReturn true
            one(queueCollection).add(equal("test"), equal(item2), equal(None), equal(Time.now), any[ClientDesc]) willReturn false
          }

          Await.result(thriftHandler.put("test", List(
            ByteBuffer.wrap(item1),
            ByteBuffer.wrap(item2),
            ByteBuffer.wrap(item3)
          ), 0)) mustEqual 1
        }
      }

      "with timeout" in {
        withFrozenThriftHandler { (thriftHandler, mutator) =>
          expect {
            one(queueCollection).add(equal("test"), equal(item1), equal(Some(5.seconds.fromNow)), equal(Time.now), any[ClientDesc]) willReturn true
          }

          Await.result(thriftHandler.put("test", List(ByteBuffer.wrap(item1)), 5000)) mustEqual 1
        }
      }
    }

    "get" in {
      "one, no timeout" in {
        withThriftHandler { thriftHandler =>
          val qitem = QItem(Time.now, None, item1, 0)

          expect {
            one(queueCollection).remove(equal("test"), equal(None), equal(false), equal(false), any[ClientDesc]) willReturn Future(Some(qitem))
          }

          Await.result(thriftHandler.get("test", 1, 0, 0)) mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 0L))
        }
      }

      "one, with timeout" in {
        withFrozenThriftHandler { (thriftHandler, mutator) =>
          val qitem = QItem(Time.now, None, item1, 0)

          expect {
            one(queueCollection).remove(equal("test"), equal(Some(1.second.fromNow)), equal(false), equal(false), any[ClientDesc]) willReturn Future(Some(qitem))
          }

          Await.result(thriftHandler.get("test", 1, 1000, 0)) mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 0L))
        }
      }

      "one, reliably" in {
        withThriftHandler { thriftHandler =>
          val qitem = QItem(Time.now, None, item1, 1)

          expect {
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem))
          }

          Await.result(thriftHandler.get("test", 1, 0, 500)) mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 1L))
        }
      }

      "multiple" in {
        withThriftHandler { thriftHandler =>
          val qitem1 = QItem(Time.now, None, item1, 1)
          val qitem2 = QItem(Time.now, None, item2, 2)

          expect {
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem1))
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem2))
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(None)
          }

          Await.result(thriftHandler.get("test", 5, 0, 500)) mustEqual List(
            thrift.Item(ByteBuffer.wrap(item1), 1L),
            thrift.Item(ByteBuffer.wrap(item2), 2L)
          )
        }
      }

      "multiple queues" in {
        withThriftHandler { thriftHandler =>
          val qitem1 = QItem(Time.now, None, item1, 1)
          val qitem2 = QItem(Time.now, None, item2, 1)

          expect {
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem1))
            one(queueCollection).remove(equal("spam"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem2))
          }

          Await.result(thriftHandler.get("test", 1, 0, 500)) mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 1L))
          Await.result(thriftHandler.get("spam", 1, 0, 500)) mustEqual List(thrift.Item(ByteBuffer.wrap(item2), 2L))
        }
      }

      "too many open transations" in {
        withThriftHandler { thriftHandler =>
          val qitems = (1 to 10).map { i => QItem(Time.now, None, item1, i) }
          expect {
            qitems.foreach { qitem =>
              one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem))
            }
          }

          Await.result(thriftHandler.get("test", 10, 0, 500)) mustEqual (1 to 10).map { i =>
            thrift.Item(ByteBuffer.wrap(item1), i.toLong)
          }

          Await.result(thriftHandler.get("test", 10, 0, 500)) mustEqual List()
        }
      }
    }

    "confirm" in {
      withThriftHandler { thriftHandler =>
        expect {
          one(queueCollection).confirmRemove("test", 2)
          one(queueCollection).confirmRemove("test", 3)
        }

        thriftHandler.handler.addPendingRead("test", 2)
        thriftHandler.handler.addPendingRead("test", 3)
        thriftHandler.confirm("test", Set(1L, 2L))
      }
    }

    "abort" in {
      withThriftHandler { thriftHandler =>
        expect {
          one(queueCollection).unremove("test", 2)
          one(queueCollection).unremove("test", 3)
        }

        thriftHandler.handler.addPendingRead("test", 2)
        thriftHandler.handler.addPendingRead("test", 3)
        thriftHandler.abort("test", Set(1L, 2L))
      }
    }

    "auto-abort" in {
      "one" in {
        withFrozenThriftHandler { (thriftHandler, time) =>
          val qitem = QItem(Time.now, None, item1, 1)

          expect {
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem))
            one(queueCollection).unremove("test", 1)
          }

          Await.result(thriftHandler.get("test", 1, 0, 500)) mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 1L))

          time.advance(501.milliseconds)
          timer.tick()
        }
      }

      "multiple" in {
        withFrozenThriftHandler { (thriftHandler, time) =>
          val qitem1 = QItem(Time.now, None, item1, 1)
          val qitem2 = QItem(Time.now, None, item2, 2)

          expect {
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem1))
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem2))
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(None)
            one(queueCollection).unremove("test", 1)
            one(queueCollection).unremove("test", 2)
          }

          Await.result(thriftHandler.get("test", 5, 0, 500)) mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 1L),
                                                                thrift.Item(ByteBuffer.wrap(item2), 2L))

          time.advance(501.milliseconds)
          timer.tick()
        }
      }

      "cleared by manual abort" in {
        withFrozenThriftHandler { (thriftHandler, time) =>
          val qitem = QItem(Time.now, None, item1, 1)

          expect {
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem))
            one(queueCollection).unremove("test", 1)
          }

          Await.result(thriftHandler.get("test", 1, 0, 500)) mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 1L))
          Await.result(thriftHandler.abort("test", Set(1L))) mustEqual 1

          timer.tasks.size mustEqual 0
        }
      }

      "cleared by confirm" in {
        withFrozenThriftHandler { (thriftHandler, time) =>
          val qitem = QItem(Time.now, None, item1, 1)

          expect {
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem))
            one(queueCollection).confirmRemove("test", 1)
          }

          Await.result(thriftHandler.get("test", 1, 0, 500)) mustEqual List(thrift.Item(ByteBuffer.wrap(item1), 1L))
          Await.result(thriftHandler.confirm("test", Set(1L))) mustEqual 1

          timer.tasks.size mustEqual 0
        }
      }

      "multiple, some confirmed" in {
        withFrozenThriftHandler { (thriftHandler, time) =>
          val qitem1 = QItem(Time.now, None, item1, 1)
          val qitem2 = QItem(Time.now, None, item2, 2)
          val qitem3 = QItem(Time.now, None, item3, 3)

          expect {
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem1))
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem2))
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem3))
            one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future(None)
            one(queueCollection).confirmRemove("test", 1)
            one(queueCollection).unremove("test", 2)
            one(queueCollection).confirmRemove("test", 3)
          }

          Await.result(thriftHandler.get("test", 5, 0, 500))
          Await.result(thriftHandler.confirm("test", Set(1L, 3L))) mustEqual 2

          timer.tasks.size mustEqual 1

          time.advance(501.milliseconds)
          timer.tick()

          timer.tasks.size mustEqual 0
        }
      }
    }

    "peek" in {
      withThriftHandler { thriftHandler =>
        val qitem1 = QItem(Time.now, None, item1, 0)

        expect {
          one(queueCollection).remove(equal("test"), equal(None), equal(false), equal(true), any[ClientDesc]) willReturn Future(Some(qitem1))
          one(queueCollection).stats("test") willReturn List(
            ("items", "10"),
            ("bytes", "10240"),
            ("logsize", "29999"),
            ("age", "500"),
            ("waiters", "2"),
            ("open_transactions", "1")
          ).toArray
        }

        val qinfo = thrift.QueueInfo(Some(ByteBuffer.wrap(item1)), 10, 10240, 29999, 500, 2, 1)
        Await.result(thriftHandler.peek("test")) mustEqual qinfo
      }
    }

    "flush_queue" in {
      withThriftHandler { thriftHandler =>
        expect {
          one(queueCollection).flush(equal("test"), any[ClientDesc])
        }

        thriftHandler.flushQueue("test")
      }
    }

    "delete_queue" in {
      withThriftHandler { thriftHandler =>
        expect {
          one(queueCollection).delete(equal("test"), any[ClientDesc])
        }

        thriftHandler.deleteQueue("test")
      }
    }

    "get_version" in {
      withThriftHandler { thriftHandler =>
        val runtime = RuntimeEnvironment(this, Array())
        Kestrel.runtime = runtime
        Await.result(thriftHandler.getVersion()) must haveClass[String]
      }
    }

    "flush_all_queues" in {
      withThriftHandler { thriftHandler =>
        expect {
          one(queueCollection).queueNames willReturn List("test", "spam")
          one(queueCollection).flush(equal("test"), any[ClientDesc])
          one(queueCollection).flush(equal("spam"), any[ClientDesc])
        }

        thriftHandler.flushAllQueues()
      }
    }

    "current_status" in {
      "handle server sets not configured" in {
        withThriftHandler { thriftHandler =>
          thriftHandler.handler.serverStatus mustEqual None
          Await.result(thriftHandler.currentStatus()) mustEqual TStatus.NotConfigured
        }
      }

      "handle server sets marked down" in {
        withServerStatusThriftHandler { (thriftHandler, serverStatus) =>
          expect {
            one(serverStatus).status willReturn Down
          }

          Await.result(thriftHandler.currentStatus()) mustEqual TStatus.NotConfigured
        }
      }

      "return current status" in {
        withServerStatusThriftHandler { (thriftHandler, serverStatus) =>
          Map(Up -> TStatus.Up,
              ReadOnly -> TStatus.ReadOnly,
              Quiescent -> TStatus.Quiescent).foreach { case (status, thriftStatus) =>
            expect {
              one(serverStatus).status willReturn status
            }

            Await.result(thriftHandler.currentStatus()) mustEqual thriftStatus
          }
        }
      }
    }

    "set_status" in {
      "throw if server sets are not configured" in {
        withThriftHandler { thriftHandler =>
          thriftHandler.handler.serverStatus mustEqual None
          val future = thriftHandler.setStatus(TStatus.Up)
          future.isThrow mustEqual true
          Await.result(future) must throwA[ServerStatusNotConfiguredException]
        }
      }

      "update status" in {
        withServerStatusThriftHandler { (thriftHandler, serverStatus) =>
          Map(TStatus.Up -> Up,
              TStatus.ReadOnly -> ReadOnly,
              TStatus.Quiescent -> Quiescent).foreach { case (thriftStatus, status) =>
            expect {
              one(serverStatus).setStatus(thriftStatus.name)
            }
            Await.result(thriftHandler.setStatus(thriftStatus)) mustEqual ()
          }
        }
      }
    }
  }
}
