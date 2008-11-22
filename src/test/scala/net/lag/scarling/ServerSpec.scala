/*
 * Copyright (c) 2008 Robey Pointer <robeypointer@lag.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package net.lag.scarling

import java.io.File
import java.net.Socket
import scala.collection.Map
import net.lag.configgy.Config
import net.lag.logging.Logger
import org.specs._


object ServerSpec extends Specification with TestHelper {

  var config: Config = null

  def makeServer = {
    config = new Config
    config("host") = "localhost"
    config("port") = 22122
    config("queue_path") = canonicalFolderName
    config("max_journal_size") = 16 * 1024
    config("log.console") = true
    config("log.level") = "debug"
    config("log.filename") = "/tmp/foo"

    // make a queue specify max_items and max_age
    config("queues.weather_updates.max_items") = 1500000
    config("queues.weather_updates.max_age") = 1800

    Scarling.startup(config)
  }


  "Server" should {
    doAfter {
      Scarling.shutdown
    }

    "configure per-queue" in {
      withTempFolder {
        makeServer
        Scarling.queues.queue("starship").map(_.maxItems) mustEqual Some(Math.MAX_INT)
        Scarling.queues.queue("starship").map(_.maxAge) mustEqual Some(0)
        Scarling.queues.queue("weather_updates").map(_.maxItems) mustEqual Some(1500000)
        Scarling.queues.queue("weather_updates").map(_.maxAge) mustEqual Some(1800)
        config("queues.starship.max_items") = 9999
        Scarling.queues.queue("starship").map(_.maxItems) mustEqual Some(9999)
      }
    }

    "set and get one entry" in {
      withTempFolder {
        makeServer
        val v = (Math.random * 0x7fffffff).toInt
        val client = new TestClient("localhost", 22122)
        client.get("test_one_entry") mustEqual ""
        client.set("test_one_entry", v.toString) mustEqual "STORED"
        client.get("test_one_entry") mustEqual v.toString
        client.get("test_one_entry") mustEqual ""
      }
    }

    "set with expiry" in {
      withTempFolder {
        makeServer
        val v = (Math.random * 0x7fffffff).toInt
        val client = new TestClient("localhost", 22122)
        client.get("test_set_with_expiry") mustEqual ""
        client.set("test_set_with_expiry", (v + 2).toString, (Time.now / 1000).toInt) mustEqual "STORED"
        client.set("test_set_with_expiry", v.toString) mustEqual "STORED"
        Time.advance(1000)
        client.get("test_set_with_expiry") mustEqual v.toString
      }
    }

    "commit a transactional get" in {
      withTempFolder {
        makeServer
        val v = (Math.random * 0x7fffffff).toInt
        val client = new TestClient("localhost", 22122)
        client.set("commit", v.toString) mustEqual "STORED"

        val client2 = new TestClient("localhost", 22122)
        val client3 = new TestClient("localhost", 22122)
        var stats = client3.stats
        stats("queue_commit_items") mustEqual "1"
        stats("queue_commit_total_items") mustEqual "1"
        stats("queue_commit_bytes") mustEqual v.toString.length.toString

        client2.get("commit/open") mustEqual v.toString
        stats = client3.stats
        stats("queue_commit_items") mustEqual "0"
        stats("queue_commit_total_items") mustEqual "1"
        stats("queue_commit_bytes") mustEqual "0"

        client2.get("commit/close") mustEqual ""
        stats = client3.stats
        stats("queue_commit_items") mustEqual "0"
        stats("queue_commit_total_items") mustEqual "1"
        stats("queue_commit_bytes") mustEqual "0"

        client2.disconnect
        Thread.sleep(10)
        stats = client3.stats
        stats("queue_commit_items") mustEqual "0"
        stats("queue_commit_total_items") mustEqual "1"
        stats("queue_commit_bytes") mustEqual "0"
      }
    }

    "auto-rollback a transaction on disconnect" in {
      withTempFolder {
        makeServer
        val v = (Math.random * 0x7fffffff).toInt
        val client = new TestClient("localhost", 22122)
        client.set("auto-rollback", v.toString) mustEqual "STORED"

        val client2 = new TestClient("localhost", 22122)
        client2.get("auto-rollback/open") mustEqual v.toString
        val client3 = new TestClient("localhost", 22122)
        client3.get("auto-rollback") mustEqual ""
        var stats = client3.stats
        stats("queue_auto-rollback_items") mustEqual "0"
        stats("queue_auto-rollback_total_items") mustEqual "1"
        stats("queue_auto-rollback_bytes") mustEqual "0"

        // oops, client2 dies before committing!
        client2.disconnect
        Thread.sleep(10)
        stats = client3.stats
        stats("queue_auto-rollback_items") mustEqual "1"
        stats("queue_auto-rollback_total_items") mustEqual "1"
        stats("queue_auto-rollback_bytes") mustEqual v.toString.length.toString

        // subsequent fetch must get the same data item back.
        client3.get("auto-rollback/open") mustEqual v.toString
        stats = client3.stats
        stats("queue_auto-rollback_items") mustEqual "0"
        stats("queue_auto-rollback_total_items") mustEqual "1"
        stats("queue_auto-rollback_bytes") mustEqual "0"
      }
    }

    "auto-commit cycles of transactional gets" in {
      withTempFolder {
        makeServer
        val v = (Math.random * 0x7fffffff).toInt
        val client = new TestClient("localhost", 22122)
        client.set("auto-commit", v.toString) mustEqual "STORED"
        client.set("auto-commit", (v + 1).toString) mustEqual "STORED"
        client.set("auto-commit", (v + 2).toString) mustEqual "STORED"

        val client2 = new TestClient("localhost", 22122)
        client2.get("auto-commit/open") mustEqual v.toString
        client2.get("auto-commit/open") mustEqual (v + 1).toString
        client2.get("auto-commit/open") mustEqual (v + 2).toString
        client2.disconnect
        Thread.sleep(10)

        val client3 = new TestClient("localhost", 22122)
        client3.get("auto-commit") mustEqual (v + 2).toString

        var stats = client3.stats
        stats("queue_auto-commit_items") mustEqual "0"
        stats("queue_auto-commit_total_items") mustEqual "3"
        stats("queue_auto-commit_bytes") mustEqual "0"
      }
    }

    "age" in {
      withTempFolder {
        makeServer
        val client = new TestClient("localhost", 22122)
        client.set("test_age", "nibbler") mustEqual "STORED"
        Time.advance(1000)
        client.get("test_age") mustEqual "nibbler"
        client.stats.contains("queue_test_age_age") mustEqual true
        client.stats("queue_test_age_age").toInt >= 1000 mustEqual true
      }
    }

    "rotate logs" in {
      withTempFolder {
        makeServer
        var v = "x"
        for (val i <- 1.to(13)) { v = v + v }    // 8192

        val client = new TestClient("localhost", 22122)

        client.set("test_log_rotation", v) mustEqual "STORED"
        new File(folderName + "/test_log_rotation").length mustEqual 8192 + 16 + 5
        client.get("test_log_rotation") mustEqual v
        new File(folderName + "/test_log_rotation").length mustEqual 8192 + 16 + 5 + 1

        client.get("test_log_rotation") mustEqual ""
        new File(folderName + "/test_log_rotation").length mustEqual 8192 + 16 + 5 + 1

        client.set("test_log_rotation", v) mustEqual "STORED"
        new File(folderName + "/test_log_rotation").length mustEqual 2 * (8192 + 16 + 5) + 1
        client.get("test_log_rotation") mustEqual v
        new File(folderName + "/test_log_rotation").length mustEqual 5
        new File(folderName).listFiles.length mustEqual 1
      }
    }

    "collect stats" in {
      withTempFolder {
        makeServer
        val client = new TestClient("localhost", 22122)
        val stats = client.stats
        val basicStats = Array("bytes", "time", "limit_maxbytes", "cmd_get", "version",
                               "bytes_written", "cmd_set", "get_misses", "total_connections",
                               "curr_connections", "curr_items", "uptime", "get_hits", "total_items",
                               "bytes_read")
        for (val key <- basicStats) { stats contains key mustEqual true }
      }
    }

    "return a valid response for an unknown command" in {
      withTempFolder {
        makeServer
        new TestClient("localhost", 22122).add("cheese", "swiss") mustEqual "CLIENT_ERROR"
      }
    }

    "disconnect and reconnect correctly" in {
      withTempFolder {
        makeServer
        val v = (Math.random * 0x7fffffff).toInt
        val client = new TestClient("localhost", 22122)
        client.set("disconnecting", v.toString)
        client.disconnect
        client.connect
        client.get("disconnecting") mustEqual v.toString
      }
    }
  }
}
