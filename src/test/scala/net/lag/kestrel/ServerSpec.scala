/*
 * Copyright 2009 Twitter, Inc.
 * Copyright 2009 Robey Pointer <robeypointer@gmail.com>
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

import java.io._
import java.net.Socket
import scala.collection.Map
import scala.util.Random
import com.twitter.Time
import com.twitter.conversions.si._
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import net.lag.TestHelper
import org.specs.Specification
import config._

class ServerSpec extends Specification with TestHelper {
  val PORT = 22199

  def makeServer = {
    val defaultConfig = new QueueConfig("") {
      maxJournalSize = 16.kilo
    }
    // make a queue specify max_items and max_age
    val weatherUpdatesConfig = new QueueConfig("weather_updates") {
      maxItems = 1500000
      maxAge = 1800.seconds
    }
    val kestrel = new Kestrel(new QueueConfig(null), List(weatherUpdatesConfig), 4, "localhost",
                              PORT, canonicalFolderName, Protocol.Ascii, Time.never, Time.never, 1)
    kestrel.start(null)
  }


  "Server" should {
    doAfter {
      Kestrel.kestrel.shutdown()
    }

    "configure per-queue" in {
      withTempFolder {
        makeServer
        val starship = Kestrel.kestrel.queueCollection("starship").get
        val weatherUpdates = Kestrel.kestrel.queueCollection("weather_updates").get
        starship.config.maxItems mustEqual Some(Int.MaxValue)
        starship.config.maxAge mustEqual Some(0)
        weatherUpdates.config.maxItems mustEqual Some(1500000)
        weatherUpdates.config.maxAge mustEqual Some(1800.seconds)
      }
    }

    "set and get one entry" in {
      withTempFolder {
        makeServer
        val v = (Random.nextInt * 0x7fffffff).toInt
        val client = new TestClient("localhost", PORT)
        client.get("test_one_entry") mustEqual ""
        client.set("test_one_entry", v.toString) mustEqual "STORED"
        client.get("test_one_entry") mustEqual v.toString
        client.get("test_one_entry") mustEqual ""
      }
    }

    "set with expiry" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          makeServer
          val v = (Random.nextInt * 0x7fffffff).toInt
          val client = new TestClient("localhost", PORT)
          client.get("test_set_with_expiry") mustEqual ""
          client.set("test_set_with_expiry", (v + 2).toString, Time.now.inSeconds) mustEqual "STORED"
          client.set("test_set_with_expiry", v.toString) mustEqual "STORED"
          time.advance(1.second)
          client.get("test_set_with_expiry") mustEqual v.toString
        }
      }
    }

    "set and get binary data" in {
      withTempFolder {
        makeServer
        val client = new TestClient("localhost", PORT)
        for (encodedObject <- List(5, "scrooge mcduck", new _root_.java.util.Date())) {
          val buffer = new ByteArrayOutputStream()
          new ObjectOutputStream(buffer).writeObject(encodedObject)
          client.getData("binary").size mustEqual 0
          client.setData("binary", buffer.toByteArray) mustEqual "STORED"
          val newBuffer = client.getData("binary")
          new ObjectInputStream(new ByteArrayInputStream(newBuffer)).readObject() mustEqual encodedObject
        }
      }
    }

    "commit a transactional get" in {
      withTempFolder {
        makeServer
        val v = (Random.nextInt * 0x7fffffff).toInt
        val client = new TestClient("localhost", PORT)
        client.set("commit", v.toString) mustEqual "STORED"

        val client2 = new TestClient("localhost", PORT)
        val client3 = new TestClient("localhost", PORT)
        var stats = client3.stats
        stats("queue_commit_items") mustEqual "1"
        stats("queue_commit_total_items") mustEqual "1"
        stats("queue_commit_bytes") mustEqual v.toString.length.toString

        client2.get("commit/open") mustEqual v.toString
        stats = client3.stats
        stats("queue_commit_items") mustEqual "0"
        stats("queue_commit_open_transactions") mustEqual "1"
        stats("queue_commit_total_items") mustEqual "1"
        stats("queue_commit_bytes") mustEqual "0"

        client2.get("commit/close") mustEqual ""
        stats = client3.stats
        stats("queue_commit_items") mustEqual "0"
        stats("queue_commit_open_transactions") mustEqual "0"
        stats("queue_commit_total_items") mustEqual "1"
        stats("queue_commit_bytes") mustEqual "0"

        client2.disconnect
        Thread.sleep(10)
        stats = client3.stats
        stats("queue_commit_items") mustEqual "0"
        stats("queue_commit_open_transactions") mustEqual "0"
        stats("queue_commit_total_items") mustEqual "1"
        stats("queue_commit_bytes") mustEqual "0"
      }
    }

    "abort a transactional get" in {
      withTempFolder {
        makeServer
        val v = (Random.nextInt * 0x7fffffff).toInt
        val client = new TestClient("localhost", PORT)
        client.set("abort", v.toString) mustEqual "STORED"

        val client2 = new TestClient("localhost", PORT)
        val client3 = new TestClient("localhost", PORT)
        var stats = client3.stats
        stats("queue_abort_items") mustEqual "1"
        stats("queue_abort_open_transactions") mustEqual "0"
        stats("queue_abort_total_items") mustEqual "1"
        stats("queue_abort_bytes") mustEqual v.toString.length.toString

        client2.get("abort/open") mustEqual v.toString
        stats = client3.stats
        stats("queue_abort_items") mustEqual "0"
        stats("queue_abort_open_transactions") mustEqual "1"
        stats("queue_abort_total_items") mustEqual "1"
        stats("queue_abort_bytes") mustEqual "0"

        client2.get("abort/abort") mustEqual ""
        stats = client3.stats
        stats("queue_abort_items") mustEqual "1"
        stats("queue_abort_open_transactions") mustEqual "0"
        stats("queue_abort_total_items") mustEqual "1"
        stats("queue_abort_bytes") mustEqual v.toString.length.toString
      }
    }

    "auto-rollback a transaction on disconnect" in {
      withTempFolder {
        makeServer
        val v = (Random.nextInt * 0x7fffffff).toInt
        val client = new TestClient("localhost", PORT)
        client.set("auto-rollback", v.toString) mustEqual "STORED"

        val client2 = new TestClient("localhost", PORT)
        client2.get("auto-rollback/open") mustEqual v.toString
        val client3 = new TestClient("localhost", PORT)
        client3.get("auto-rollback") mustEqual ""
        var stats = client3.stats
        stats("queue_auto-rollback_items") mustEqual "0"
        stats("queue_auto-rollback_open_transactions") mustEqual "1"
        stats("queue_auto-rollback_total_items") mustEqual "1"
        stats("queue_auto-rollback_bytes") mustEqual "0"

        // oops, client2 dies before committing!
        client2.disconnect
        waitUntil { client3.stats("queue_auto-rollback_bytes") == v.toString.length.toString } mustBe true
        stats = client3.stats
        stats("queue_auto-rollback_items") mustEqual "1"
        stats("queue_auto-rollback_open_transactions") mustEqual "0"
        stats("queue_auto-rollback_total_items") mustEqual "1"

        // subsequent fetch must get the same data item back.
        client3.get("auto-rollback/open") mustEqual v.toString
        stats = client3.stats
        stats("queue_auto-rollback_items") mustEqual "0"
        stats("queue_auto-rollback_open_transactions") mustEqual "1"
        stats("queue_auto-rollback_total_items") mustEqual "1"
        stats("queue_auto-rollback_bytes") mustEqual "0"
      }
    }

    "auto-commit cycles of transactional gets" in {
      withTempFolder {
        makeServer
        val v = (Random.nextInt * 0x7fffffff).toInt
        val client = new TestClient("localhost", PORT)
        client.set("auto-commit", v.toString) mustEqual "STORED"
        client.set("auto-commit", (v + 1).toString) mustEqual "STORED"
        client.set("auto-commit", (v + 2).toString) mustEqual "STORED"

        val client2 = new TestClient("localhost", PORT)
        client2.get("auto-commit/open") mustEqual v.toString
        client2.get("auto-commit/close/open") mustEqual (v + 1).toString
        client2.get("auto-commit/close/open") mustEqual (v + 2).toString
        client2.disconnect

        val client3 = new TestClient("localhost", PORT)
        waitUntil { client3.stats("queue_auto-commit_bytes") == v.toString.length.toString } mustBe true
        client3.get("auto-commit") mustEqual (v + 2).toString

        var stats = client3.stats
        stats("queue_auto-commit_items") mustEqual "0"
        stats("queue_auto-commit_total_items") mustEqual "3"
        stats("queue_auto-commit_bytes") mustEqual "0"
      }
    }

    "age" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          makeServer
          val client = new TestClient("localhost", PORT)
          client.set("test_age", "nibbler") mustEqual "STORED"
          client.set("test_age", "nibbler2") mustEqual "STORED"
          time.advance(1.second)
          client.get("test_age") mustEqual "nibbler"
          client.stats.contains("queue_test_age_age") mustEqual true
          client.stats("queue_test_age_age").toInt >= 1000 mustEqual true
        }
      }
    }

    "peek" in {
      withTempFolder {
        makeServer
        val client = new TestClient("localhost", PORT)
        client.set("testy", "nibbler") mustEqual "STORED"
        client.get("testy/peek/open") must throwA[ClientError]

        val client2 = new TestClient("localhost", PORT)
        client2.get("testy/peek") mustEqual "nibbler"
        client2.get("testy/peek") mustEqual "nibbler"
        client2.get("testy/peek") mustEqual "nibbler"
        client2.get("testy") mustEqual "nibbler"
        client2.get("testy") mustEqual ""
      }
    }

    "rotate logs" in {
      withTempFolder {
        makeServer
        val v = new String(new Array[Byte](8192))

        val client = new TestClient("localhost", PORT)

        client.set("test_log_rotation", v) mustEqual "STORED"
        new File(folderName + "/test_log_rotation").length mustEqual 8192 + 16 + 5
        // specs is very slow to compare long strings
        (client.get("test_log_rotation") == v) must beTrue
        new File(folderName + "/test_log_rotation").length mustEqual 8192 + 16 + 5 + 1

        client.get("test_log_rotation") mustEqual ""
        new File(folderName + "/test_log_rotation").length mustEqual 8192 + 16 + 5 + 1

        client.set("test_log_rotation", v) mustEqual "STORED"
        new File(folderName + "/test_log_rotation").length mustEqual 2 * (8192 + 16 + 5) + 1
        (client.get("test_log_rotation") == v) must beTrue
        new File(folderName + "/test_log_rotation").length mustEqual 5
        new File(folderName).listFiles.length mustEqual 1
      }
    }

    "collect stats" in {
      withTempFolder {
        makeServer
        val client = new TestClient("localhost", PORT)
        val stats = client.stats
        val basicStats = Array("bytes", "time", "cmd_get", "version",
                               "bytes_written", "cmd_set", "get_misses", "total_connections",
                               "curr_connections", "curr_items", "uptime", "get_hits", "total_items",
                               "bytes_read")
        for (key <- basicStats) { stats contains key mustEqual true }
      }
    }

    "return a valid response for an unknown command" in {
      withTempFolder {
        makeServer
        new TestClient("localhost", PORT).add("cheese", "swiss") mustEqual "CLIENT_ERROR"
      }
    }

    "disconnect and reconnect correctly" in {
      withTempFolder {
        makeServer
        val v = (Random.nextInt * 0x7fffffff).toInt
        val client = new TestClient("localhost", PORT)
        client.set("disconnecting", v.toString)
        client.disconnect
        client.connect
        client.get("disconnecting") mustEqual v.toString
      }
    }

    "flush expired items" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          makeServer
          val client = new TestClient("localhost", PORT)
          client.set("q1", "1", 1)
          client.set("q2", "2", 1)
          client.set("q2", "2", 1)
          client.set("q3", "3", 1)
          client.stats("queue_q1_items") mustEqual "1"
          client.stats("queue_q2_items") mustEqual "2"
          client.stats("queue_q3_items") mustEqual "1"

          time.advance(5.seconds)

          client.out.write("flush_expired q1\n".getBytes)
          client.readline mustEqual "1"
          client.stats("queue_q1_items") mustEqual "0"
          client.stats("queue_q2_items") mustEqual "2"
          client.stats("queue_q3_items") mustEqual "1"

          client.out.write("flush_all_expired\n".getBytes)
          client.readline mustEqual "3"
          client.stats("queue_q1_items") mustEqual "0"
          client.stats("queue_q2_items") mustEqual "0"
          client.stats("queue_q3_items") mustEqual "0"
        }
      }
    }
  }
}
