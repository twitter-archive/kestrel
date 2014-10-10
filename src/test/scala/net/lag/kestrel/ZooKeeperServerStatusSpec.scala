/*
 * Copyright 2012 Twitter, Inc.
 * Copyright 2012 Robey Pointer <robeypointer@gmail.com>
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

import com.twitter.common.zookeeper.{Group, ServerSet, ZooKeeperClient}
import com.twitter.common.zookeeper.ServerSet.EndpointStatus
import com.twitter.conversions.time._
import com.twitter.logging.TestLogging
import com.twitter.thrift.{Status => TStatus}
import com.twitter.util.{MockTimer, TempFolder, Time, TimeControl}
import java.io._
import java.net.{InetAddress, InetSocketAddress, UnknownHostException}
import org.specs.SpecificationWithJUnit
import org.specs.mock.{ClassMocker, JMocker}
import scala.collection.JavaConversions
import scala.collection.mutable.Queue
import config._

class ZooKeeperServerStatusSpec extends SpecificationWithJUnit with JMocker with ClassMocker with TempFolder
with TestLogging {
  val mockZKClient = mock[ZooKeeperClient]
  val mockZKServerSets = Map(
    "/kestrel/read"  -> Queue[ServerSet](),
    "/kestrel/write" -> Queue[ServerSet]()
  )
  var mockTimer: MockTimer = null

  val memcacheAddr = new InetSocketAddress("10.1.2.3", 22133)
  val thriftAddr = new InetSocketAddress("10.1.2.3", 2229)

  def statusFile = canonicalFolderName + "/state"

  def nextMockZKServerSet(nodeType: String): ServerSet = {
    val mockServerSet = mock[ServerSet]
    mockZKServerSets("/kestrel/" + nodeType).enqueue(mockServerSet)
    mockServerSet
  }

  def makeZooKeeperServerStatus(): ZooKeeperServerStatus = {
    val zkConfig = new ZooKeeperBuilder {
      host = "localhost"
      pathPrefix = "/kestrel"
    }

    mockTimer = new MockTimer
    val serverStatus = new ZooKeeperServerStatus(zkConfig(), statusFile, mockTimer) {
      override protected val zkClient = mockZKClient

      override protected def createServerSet(nodeType: String) = {
        mockZKServerSets("/kestrel/" + nodeType).dequeue
      }
    }
    serverStatus.start()
    serverStatus
  }

  def expectInitialEndpointStatus(memcache: Option[InetSocketAddress],
                                  thrift: Option[InetSocketAddress] = None,
                                  text: Option[InetSocketAddress] = None,
                                  initialStatus: TStatus = TStatus.DEAD):
      Tuple2[EndpointStatus, EndpointStatus] = {
    val readStatus = mock[EndpointStatus]
    val writeStatus = mock[EndpointStatus]

    expect {
      initialJoin("read", memcache, thrift, text, initialStatus) willReturn readStatus
      initialJoin("write", memcache, thrift, text, initialStatus) willReturn writeStatus
    }

    (readStatus, writeStatus)
  }

  def endpoints(memcache: Option[InetSocketAddress],
                thrift: Option[InetSocketAddress],
                text: Option[InetSocketAddress]) = {
    val sockets = memcache.map { a => "memcache" -> a } ++
                    thrift.map { a => "thrift" -> a } ++
                    text.map { a => "text" -> a }
    (sockets.head._2, sockets.toMap)
  }

  def initialJoin(nodeType: String,
                  memcache: Option[InetSocketAddress],
                  thrift: Option[InetSocketAddress] = None,
                  text: Option[InetSocketAddress] = None,
                  initialStatus: TStatus = TStatus.DEAD) = {
    val (main, eps) = endpoints(memcache, thrift, text)

    one(nextMockZKServerSet(nodeType)).join(main, JavaConversions.asJavaMap(eps), initialStatus)
  }

  def rejoin(nodeType: String,
             memcache: Option[InetSocketAddress],
             thrift: Option[InetSocketAddress] = None,
             text: Option[InetSocketAddress] = None) = {
    val (main, eps) = endpoints(memcache, thrift, text)

    one(nextMockZKServerSet(nodeType)).join(main, JavaConversions.asJavaMap(eps), TStatus.ALIVE)
  }

  def withZooKeeperServerStatus(f: (ZooKeeperServerStatus, TimeControl) => Unit) {
    withTempFolder {
      Time.withCurrentTimeFrozen { mutator =>
        val serverStatus = makeZooKeeperServerStatus()
        f(serverStatus, mutator)
      }
    }
  }

  def storedStatus(): String = {
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(statusFile), "UTF-8"))
    try {
      reader.readLine
    } finally {
      reader.close()
    }
  }

  def writeStatus(status: Status) {
    new File(statusFile).getParentFile.mkdirs()
    val writer = new OutputStreamWriter(new FileOutputStream(statusFile), "UTF-8")
    try {
      writer.write(status.toString)
    } finally {
      writer.close()
    }
  }

  "ZooKeeperServerStatus" should {
    "status" in {
      "close zookeeper client on shutdown" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          serverStatus.markUp()
          serverStatus.status mustEqual Up

          expect {
            one(mockZKClient).close()
          }

          serverStatus.shutdown()
          serverStatus.status mustEqual Down
        }
      }

      "throw on unsuccessful change" in {
        withZooKeeperServerStatus { (serverStatus, tc) =>
          serverStatus.markQuiescent()
          storedStatus() mustEqual "Quiescent"

          tc.advance(31.seconds)
          mockTimer.tick()

          val (readStatus, writeStatus) = expectInitialEndpointStatus(Some(memcacheAddr))

          serverStatus.addEndpoints("memcache", Map("memcache" -> memcacheAddr))

          expect {
            rejoin("read", Some(memcacheAddr)) willThrow new Group.JoinException("boom", new Exception)
          }

          serverStatus.markUp() must throwA[Group.JoinException]
          storedStatus() mustEqual "Quiescent"
        }
      }
    }

    "server set memberships" in {
      "rejoin all existing serversets when status changes from dead to alive" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          val (readStatus, writeStatus) = expectInitialEndpointStatus(Some(memcacheAddr), Some(thriftAddr))

          serverStatus.addEndpoints("memcache", Map("memcache" -> memcacheAddr, "thrift" -> thriftAddr))

          expect {
            rejoin("write", Some(memcacheAddr), Some(thriftAddr)) willReturn mock[EndpointStatus]
            rejoin("read", Some(memcacheAddr), Some(thriftAddr)) willReturn mock[EndpointStatus]
          }

          serverStatus.markUp()
        }
      }

      "join serversets with newly added endpoints with the current status" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          serverStatus.markUp()

          val (readStatus, writeStatus) =
            expectInitialEndpointStatus(Some(memcacheAddr), Some(thriftAddr), initialStatus = TStatus.ALIVE)

          serverStatus.addEndpoints("memcache", Map("memcache" -> memcacheAddr, "thrift" -> thriftAddr))
        }
      }

      "update all existing serversets when status changes from alive to dead" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          serverStatus.markUp()

          val (readStatus, writeStatus) =
            expectInitialEndpointStatus(Some(memcacheAddr), Some(thriftAddr), initialStatus = TStatus.ALIVE)

          serverStatus.addEndpoints("memcache", Map("memcache" -> memcacheAddr, "thrift" -> thriftAddr))

          expect {
            one(writeStatus).update(TStatus.DEAD)
            one(readStatus).update(TStatus.DEAD)
          }

          serverStatus.markQuiescent()
        }
      }

      "skip updating with unchanged status" in {
        withZooKeeperServerStatus { (serverStatus, tc) =>
          serverStatus.markUp()

          val (readStatus, writeStatus) =
            expectInitialEndpointStatus(Some(memcacheAddr), Some(thriftAddr), initialStatus = TStatus.ALIVE)

          serverStatus.addEndpoints("memcache", Map("memcache" -> memcacheAddr, "thrift" -> thriftAddr))

          expect {
            one(writeStatus).update(TStatus.DEAD)
          }

          serverStatus.markReadOnly()

          tc.advance(31.seconds)
          mockTimer.tick()

          expect {
            one(readStatus).update(TStatus.DEAD)
          }

          serverStatus.markQuiescent()
        }
      }

    }

    "adding endpoints" in {
      val hostAddr = ZooKeeperIP.toExternalAddress(InetAddress.getByAddress(Array[Byte](0, 0, 0, 0)))
      val expectedAddr = new InetSocketAddress(hostAddr, 22133)

      "should convert wildcard addresses to a proper local address" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          val wildcardAddr = new InetSocketAddress(22133)
          val (readStatus, writeStatus) = expectInitialEndpointStatus(Some(expectedAddr))

          serverStatus.addEndpoints("memcache", Map("memcache" -> wildcardAddr))
        }
      }

      "should explode if given the loopback address" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          val localhostAddr = new InetSocketAddress("localhost", 22133)

          serverStatus.addEndpoints("memcache", Map("memcache" -> localhostAddr)) must throwA[UnknownHostException]
        }
      }

      "should leave non-wildcard, non-loopback addresses alone" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          val addr = new InetSocketAddress("1.2.3.4", 22133)
          val (readStatus, writeStatus) = expectInitialEndpointStatus(Some(addr))
          serverStatus.addEndpoints("memcache", Map("memcache" -> addr))
        }
      }
    }
  }
}
