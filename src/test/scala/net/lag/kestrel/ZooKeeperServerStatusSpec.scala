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

import com.twitter.common.zookeeper.{ServerSet, ZooKeeperClient}
import com.twitter.common.zookeeper.ServerSet.EndpointStatus
import com.twitter.conversions.time._
import com.twitter.logging.TestLogging
import com.twitter.thrift.{Status => TStatus}
import com.twitter.util.{MockTimer, TempFolder, Time, TimeControl}
import java.io._
import java.net.{InetAddress, InetSocketAddress}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import scala.collection.JavaConversions
import config._

class ZooKeeperServerStatusSpec extends Specification with JMocker with ClassMocker with TempFolder
with TestLogging {
  val mockZKClient = mock[ZooKeeperClient]
  val mockZKServerSets = Map(
    "/kestrel/read"  -> mock[ServerSet],
    "/kestrel/write" -> mock[ServerSet]
  )
  var mockTimer: MockTimer = null

  val memcacheAddr = new InetSocketAddress("10.1.2.3", 22133)
  val thriftAddr = new InetSocketAddress("10.1.2.3", 2229)

  def statusFile = canonicalFolderName + "/state"

  def makeZooKeeperServerStatus(): ZooKeeperServerStatus = {
    val zkConfig = new ZooKeeperBuilder {
      host = "localhost"
      pathPrefix = "/kestrel"
    }

    mockTimer = new MockTimer
    val serverStatus = new ZooKeeperServerStatus(zkConfig(), statusFile, mockTimer) {
      override protected val zkClient = mockZKClient

      override protected def createServerSet(nodeType: String) = {
        mockZKServerSets("/kestrel/" + nodeType)
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

    val sockets = memcache.map { a => "memcache" -> a } ++
                  thrift.map { a => "thrift" -> a } ++
                  text.map { a => "text" -> a }
    val addr = sockets.head._2
    val endpoints = JavaConversions.asJavaMap(sockets.toMap)

    expect {
      one(mockZKServerSets("/kestrel/read")).join(addr, endpoints, initialStatus) willReturn readStatus
      one(mockZKServerSets("/kestrel/write")).join(addr, endpoints, initialStatus) willReturn writeStatus
    }
    (readStatus, writeStatus)
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
            one(readStatus).update(TStatus.ALIVE) willThrow new ServerSet.UpdateException("boom")
          }

          serverStatus.markUp() must throwA[ServerSet.UpdateException]
          storedStatus() mustEqual "Quiescent"
        }
      }
    }

    "server set memberships" in {
      "update all existing endpoints when status changes" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          val (readStatus, writeStatus) = expectInitialEndpointStatus(Some(memcacheAddr), Some(thriftAddr))

          serverStatus.addEndpoints("memcache", Map("memcache" -> memcacheAddr, "thrift" -> thriftAddr))

          expect {
            one(writeStatus).update(TStatus.ALIVE)
            one(readStatus).update(TStatus.ALIVE)
          }

          serverStatus.markUp()
        }
      }

      "update newly added endpoints to the current status" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          serverStatus.markUp()

          val (readStatus, writeStatus) =
            expectInitialEndpointStatus(Some(memcacheAddr), Some(thriftAddr), initialStatus = TStatus.ALIVE)

          serverStatus.addEndpoints("memcache", Map("memcache" -> memcacheAddr, "thrift" -> thriftAddr))
        }
      }
    }

    "adding endpoints" in {
      val expectedAddr = new InetSocketAddress(InetAddress.getLocalHost.getHostAddress, 22133)
      expectedAddr.getAddress.isLoopbackAddress mustEqual false

      "should convert wildcard addresses to a proper local address" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          val wildcardAddr = new InetSocketAddress(22133)
          val (readStatus, writeStatus) = expectInitialEndpointStatus(Some(expectedAddr))

          serverStatus.addEndpoints("memcache", Map("memcache" -> wildcardAddr))
        }
      }

      "should convert the loopback address to a proper local address" in {
        withZooKeeperServerStatus { (serverStatus, _) =>
          val localhostAddr = new InetSocketAddress("localhost", 22133)
          val (readStatus, writeStatus) = expectInitialEndpointStatus(Some(expectedAddr))

          serverStatus.addEndpoints("memcache", Map("memcache" -> localhostAddr))
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
