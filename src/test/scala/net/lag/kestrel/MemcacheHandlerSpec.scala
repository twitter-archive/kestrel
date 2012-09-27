/*
 * Copyright 2012 Twitter, Inc.
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
import com.twitter.naggati.{Codec, LatchedChannelSource}
import com.twitter.naggati.codec.{MemcacheRequest, MemcacheResponse}
import com.twitter.ostrich.admin.RuntimeEnvironment
import com.twitter.util.{Future, Promise, Time}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import scala.collection.mutable

class MemcacheHandlerSpec extends Specification with JMocker with ClassMocker {
  type ClientDesc = Option[() => String]

  "MemcacheHandler" should {
    val queueCollection = mock[QueueCollection]
    val connection = mock[ClientConnection]
    val address = new InetSocketAddress("", 0)
    val qitem = QItem(Time.now, None, "state shirt".getBytes, 23)

    def toReq(command: String, payload: Option[String] = None): MemcacheRequest = {
      val parts = command.split(" ").toList
      val data = payload map { s =>
        ByteBuffer.wrap(s.getBytes)
      }
      val dataLength = data map { _.remaining + 2 } orElse { Some(0) }
      MemcacheRequest(parts, data, command.length + 2 + dataLength.get)
    }

    def toResp(queue: String, qItem: QItem): MemcacheResponse = {
      MemcacheResponse("VALUE %s 0 %d".format(queue, qItem.data.length), Some(ByteBuffer.wrap(qItem.data)))
    }

    val endResponse = MemcacheResponse("END", None)
    val errorResponse = MemcacheResponse("ERROR", None)
    val clientErrorResponse = MemcacheResponse("CLIENT_ERROR", None)

    "get request (transactional)" in {
      expect {
        1.atLeastOf(connection).remoteAddress willReturn address
      }

      val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)

      "closes transactions" in {
        expect {
          one(queueCollection).confirmRemove("test", 100)
          one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem))
        }

        memcacheHandler.handler.pendingReads.add("test", 100)
        memcacheHandler.handler.pendingReads.peek("test") mustEqual List(100)
        memcacheHandler(toReq("get test/close/open"))() mustEqual toResp("test", qitem)
        memcacheHandler.handler.pendingReads.peek("test") mustEqual List(qitem.xid)
      }

      "with timeout" in {
        "value ready immediately" in {
          Time.withCurrentTimeFrozen { time =>
            expect {
              one(queueCollection).remove(equal("test"), equal(Some(500.milliseconds.fromNow)), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem))
            }

            memcacheHandler(toReq("get test/t=500/close/open"))() mustEqual toResp("test", qitem)
            memcacheHandler.handler.pendingReads.peek("test") mustEqual List(qitem.xid)
          }
        }

        "value ready eventually" in {
          Time.withCurrentTimeFrozen { time =>
            val promise = new Promise[Option[QItem]]

            expect {
              one(queueCollection).remove(equal("test"), equal(Some(500.milliseconds.fromNow)), equal(true), equal(false), any[ClientDesc]) willReturn promise
            }

            val future = memcacheHandler(toReq("get test/t=500/close/open"))

            promise.setValue(Some(qitem))
            future() mustEqual toResp("test", qitem)
            memcacheHandler.handler.pendingReads.peek("test") mustEqual List(qitem.xid)
          }
        }

        "timed out" in {
          Time.withCurrentTimeFrozen { time =>
            val promise = new Promise[Option[QItem]]

            expect {
              one(queueCollection).confirmRemove("test", 100)
              one(queueCollection).remove(equal("test"), equal(Some(500.milliseconds.fromNow)), equal(true), equal(false), any[ClientDesc]) willReturn promise
            }

            memcacheHandler.handler.pendingReads.add("test", 100)
            memcacheHandler.handler.pendingReads.peek("test") mustEqual List(100)

            val future = memcacheHandler(toReq("get test/t=500/close/open"))

            promise.setValue(None)
            future() mustEqual endResponse
            memcacheHandler.handler.pendingReads.peek("test") mustEqual List()
          }
        }
      }

      "empty queue" in {
        expect {
          one(queueCollection).confirmRemove("test", 100)
          one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(None)
        }

        memcacheHandler.handler.pendingReads.add("test", 100)
        memcacheHandler.handler.pendingReads.peek("test") mustEqual List(100)
        memcacheHandler(toReq("get test/close/open"))() mustEqual endResponse
        memcacheHandler.handler.pendingReads.peek("test") mustEqual List()
      }

      "item ready" in {
        expect {
          one(queueCollection).remove(equal("test"), equal(None), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem))
        }

        memcacheHandler(toReq("get test/close/open"))() mustEqual toResp("test", qitem)
        memcacheHandler.handler.pendingReads.peek("test") mustEqual List(qitem.xid)
      }

      "aborting" in {
        memcacheHandler.handler.pendingReads.add("test", 100)
        memcacheHandler.handler.pendingReads.peek("test") mustEqual List(100)

        expect {
          one(queueCollection).unremove("test", 100)
        }

        memcacheHandler(toReq("get test/abort"))() mustEqual endResponse
        memcacheHandler.handler.pendingReads.peek("test") mustEqual List()
      }

      "forbidden option combinations" in {
        memcacheHandler(toReq("get test/open/peek"))() mustEqual clientErrorResponse
        memcacheHandler(toReq("get test/close/peek"))() mustEqual clientErrorResponse
        memcacheHandler(toReq("get test/open/abort"))() mustEqual clientErrorResponse
        memcacheHandler(toReq("get test/close/abort"))() mustEqual clientErrorResponse
      }

      "queue name required" in {
        memcacheHandler(toReq("get /close/open"))() mustEqual clientErrorResponse
      }
    }

    "get request (non-transactional)" in {
      expect {
        1.atLeastOf(connection).remoteAddress willReturn address
      }

      val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)

      "with timeout" in {
        "value ready immediately" in {
          Time.withCurrentTimeFrozen { time =>
            expect {
              one(queueCollection).remove(equal("test"), equal(Some(500.milliseconds.fromNow)), equal(false), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem))
            }

            memcacheHandler(toReq("get test/t=500"))() mustEqual toResp("test", qitem)
            memcacheHandler.handler.pendingReads.peek("test") mustEqual List()
          }
        }

        "value ready eventually" in {
          Time.withCurrentTimeFrozen { time =>
            val promise = new Promise[Option[QItem]]

            expect {
              one(queueCollection).remove(equal("test"), equal(Some(500.milliseconds.fromNow)), equal(false), equal(false), any[ClientDesc]) willReturn promise
            }

            val future = memcacheHandler(toReq("get test/t=500"))

            promise.setValue(Some(qitem))
            future() mustEqual toResp("test", qitem)
            memcacheHandler.handler.pendingReads.peek("test") mustEqual List()
          }
        }
      }

      "item ready" in {
        expect {
          one(queueCollection).remove(equal("test"), equal(None), equal(false), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem))
        }

        memcacheHandler(toReq("get test"))() mustEqual toResp("test", qitem)
        memcacheHandler.handler.pendingReads.peek("test") mustEqual List()
      }

      "peek" in {
        expect {
          one(queueCollection).remove(equal("test"), equal(None), equal(false), equal(true), any[ClientDesc]) willReturn Future.value(Some(qitem))
        }

        memcacheHandler(toReq("get test/peek"))() mustEqual toResp("test", qitem)
      }
    }

    "monitor request" in {
      val qitem2 = QItem(Time.now, None, "homunculus".getBytes, 24)
      val qitem3 = QItem(Time.now, None, "automaton".getBytes, 25)

      def responseStreamToList(response: MemcacheResponse): List[MemcacheResponse] = {
        val received = new mutable.ListBuffer[MemcacheResponse]
        val signals = response.signals
        signals.length mustEqual 1
        signals.head must haveClass[Codec.Stream[MemcacheResponse]]

        val codecStream = signals.head.asInstanceOf[Codec.Stream[MemcacheResponse]]
        codecStream.stream respond { r =>
          received += r
          Future.Done
        }
        received.toList
      }

      "items ready" in {
        Time.withCurrentTimeFrozen { tc =>
          val timeLimit = Some(Time.now + 100.seconds)
          expect {
            one(connection).remoteAddress willReturn address
            one(queueCollection).remove(equal("test"), equal(timeLimit), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem))
            one(queueCollection).remove(equal("test"), equal(timeLimit), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem2))
            one(queueCollection).remove(equal("test"), equal(timeLimit), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem3))
          }
          val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)

          val response = memcacheHandler(toReq("monitor test 100 3"))()
          val received = responseStreamToList(response)
          received mustEqual List(qitem, qitem2, qitem3).map { i => toResp("test", i) } ++ List(endResponse)
        }
      }

      "items eventually ready" in {
        Time.withCurrentTimeFrozen { tc =>
          val timeLimit = Some(Time.now + 100.seconds)
          val promise = new Promise[Option[QItem]]
          val promise2 = new Promise[Option[QItem]]
          expect {
            one(connection).remoteAddress willReturn address
            one(queueCollection).remove(equal("test"), equal(timeLimit), equal(true), equal(false), any[ClientDesc]) willReturn promise
            one(queueCollection).remove(equal("test"), equal(timeLimit), equal(true), equal(false), any[ClientDesc]) willReturn promise2
          }
          val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)
          val response = memcacheHandler(toReq("monitor test 100 2"))

          tc.advance(1.second)
          promise.setValue(Some(qitem))
          tc.advance(1.second)
          promise2.setValue(Some(qitem2))

          val received = responseStreamToList(response())
          received mustEqual List(qitem, qitem2).map { i => toResp("test", i) } ++ List(endResponse)
        }
      }

      "some items ready" in {
        Time.withCurrentTimeFrozen { tc =>
          val timeLimit = Some(Time.now + 100.seconds)
          expect {
            one(connection).remoteAddress willReturn address
            one(queueCollection).remove(equal("test"), equal(timeLimit), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem))
            one(queueCollection).remove(equal("test"), equal(timeLimit), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem2))
            one(queueCollection).remove(equal("test"), equal(timeLimit), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(Some(qitem3))
            one(queueCollection).remove(equal("test"), equal(timeLimit), equal(true), equal(false), any[ClientDesc]) willReturn Future.value(None)
          }
          val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)

          val response = memcacheHandler(toReq("monitor test 100 5"))()
          val received = responseStreamToList(response)
          received mustEqual List(qitem, qitem2, qitem3).map { i => toResp("test", i) } ++ List(endResponse)
        }
      }

      "timeout" in {
        Time.withCurrentTimeFrozen { tc =>
          val timeLimit = Some(Time.now + 100.seconds)
          val promise = new Promise[Option[QItem]]

          expect {
            one(connection).remoteAddress willReturn address
            one(queueCollection).remove(equal("test"), equal(timeLimit), equal(true), equal(false), any[ClientDesc]) willReturn promise
          }
          val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)
          val response = memcacheHandler(toReq("monitor test 100 2"))

          tc.advance(101.seconds)
          promise.setValue(Some(qitem))

          val received = responseStreamToList(response())
          received mustEqual List(toResp("test", qitem), endResponse)
        }
      }

      "max open reads" in {
        Time.withCurrentTimeFrozen { tc =>
          val timeLimit = Some(Time.now + 100.seconds)

          expect {
            one(connection).remoteAddress willReturn address
            one(queueCollection).remove(equal("test"), equal(timeLimit), equal(true), equal(false), any[ClientDesc]) willReturn Future(Some(qitem))
          }
          val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)
          (100 until 109).foreach { xid =>
            memcacheHandler.handler.pendingReads.add("test", xid)
          }
          memcacheHandler.handler.pendingReads.size("test") mustEqual 9

          val response = memcacheHandler(toReq("monitor test 100 2"))

          val received = responseStreamToList(response())
          received mustEqual List(toResp("test", qitem), endResponse)
        }
      }
    }

    "confirm" in {
      "single" in {
        expect {
          one(connection).remoteAddress willReturn address
          one(queueCollection).confirmRemove("test", 100)
        }

        val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)
        memcacheHandler.handler.pendingReads.add("test", 100)
        memcacheHandler(toReq("confirm test 1"))() mustEqual MemcacheResponse("END", None)
        memcacheHandler.handler.pendingReads.size("test") mustEqual 0
      }

      "multiple" in {
        expect {
          one(connection).remoteAddress willReturn address
          one(queueCollection).confirmRemove("test", 100)
          one(queueCollection).confirmRemove("test", 101)
        }

        val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)
        memcacheHandler.handler.pendingReads.add("test", 100)
        memcacheHandler.handler.pendingReads.add("test", 101)
        memcacheHandler.handler.pendingReads.add("test", 102)
        memcacheHandler(toReq("confirm test 2"))() mustEqual MemcacheResponse("END", None)
        memcacheHandler.handler.pendingReads.peek("test") mustEqual List(102)
      }
    }

    "put request" in {
      Time.withCurrentTimeFrozen { timeMutator =>
        expect {
          one(connection).remoteAddress willReturn address
          one(queueCollection).add(equal("test"), equal("hello".getBytes), equal(None), equal(Time.now), any[ClientDesc]) willReturn true
        }

        val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)
        memcacheHandler(toReq("set test 0 0 5", Some("hello")))() mustEqual MemcacheResponse("STORED", None)
      }
    }

    "delete request" in {
      expect {
        one(connection).remoteAddress willReturn address
        one(queueCollection).delete(equal("test"), any[ClientDesc])
      }

      val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)
      memcacheHandler(toReq("delete test"))() mustEqual MemcacheResponse("DELETED", None)
    }

    "flush request" in {
      expect {
        one(connection).remoteAddress willReturn address
        one(queueCollection).flush(equal("test"), any[ClientDesc])
      }

      val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)
      memcacheHandler(toReq("flush test"))() mustEqual MemcacheResponse("END", None)
    }

    "version request" in {
      expect {
        one(connection).remoteAddress willReturn address
      }

      val runtime = RuntimeEnvironment(this, Array())
      Kestrel.runtime = runtime

      val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)
      val response = memcacheHandler(toReq("version"))()
      response.line mustEqual ("VERSION " + runtime.jarVersion)
    }

    "status request" in {
      "without server status" in {
        expect {
          one(connection).remoteAddress willReturn address
        }

        val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)

        "check status should return an error" in {
          memcacheHandler(toReq("status"))() mustEqual errorResponse
        }

        "set status should return an error" in {
          memcacheHandler(toReq("status up"))() mustEqual errorResponse
        }
      }

      "with server status" in {
        val serverStatus = mock[ServerStatus]

        expect {
          one(connection).remoteAddress willReturn address
        }

        val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10, Some(serverStatus))

        "check status should return current status" in {
          expect {
            one(serverStatus).status willReturn Up
          }

          memcacheHandler(toReq("status"))() mustEqual MemcacheResponse("UP", None)
        }

        "set status should set current status" in {
          expect {
            one(serverStatus).setStatus("readonly")
          }

          memcacheHandler(toReq("status readonly"))() mustEqual endResponse
        }

        "set status should report client error on invalid status" in {
          expect {
            one(serverStatus).setStatus("spongebob") willThrow new UnknownStatusException
          }

          memcacheHandler(toReq("status spongebob"))() mustEqual clientErrorResponse
        }
      }
    }

    "unknown command" in {
      expect {
        one(connection).remoteAddress willReturn address
      }

      val memcacheHandler = new MemcacheHandler(connection, queueCollection, 10)
      memcacheHandler(toReq("die in a fire"))() mustEqual clientErrorResponse
    }

    // FIXME: shutdown
  }
}
