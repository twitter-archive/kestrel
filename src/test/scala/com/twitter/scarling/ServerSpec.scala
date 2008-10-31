/** Copyright 2008 Twitter, Inc. */
package com.twitter.scarling

import java.io.File
import java.net.Socket
import scala.collection.Map
import net.lag.configgy.Config
import net.lag.logging.Logger
import org.specs._


object ServerSpec extends Specification with TestHelper {

  def makeServer = {
    val config = new Config
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
        Scarling.queues.queue("starship").map(_.config("max_age", 0)) mustEqual Some(0)
        Scarling.queues.queue("starship").map(_.config("max_age", 0)) mustEqual Some(0)
        Scarling.queues.queue("weather_updates").map(_.config("max_items", 0)) mustEqual Some(1500000)
        Scarling.queues.queue("weather_updates").map(_.config("max_age", 0)) mustEqual Some(1800)
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
        client.set("test_set_with_epxiry", (v + 2).toString, (System.currentTimeMillis / 1000).toInt) mustEqual "STORED"
        client.set("test_set_with_expiry", v.toString) mustEqual "STORED"
        Thread.sleep(1000)
        client.get("test_set_with_expiry") mustEqual v.toString
      }
    }

    "age" in {
      withTempFolder {
        makeServer
        val client = new TestClient("localhost", 22122)
        client.set("test_age", "nibbler") mustEqual "STORED"
        Thread.sleep(1000)
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
        new File(folderName + "/test_log_rotation").length mustEqual 8201
        client.get("test_log_rotation") mustEqual v
        new File(folderName + "/test_log_rotation").length mustEqual 8202

        client.get("test_log_rotation") mustEqual ""
        new File(folderName + "/test_log_rotation").length mustEqual 8202

        client.set("test_log_rotation", v) mustEqual "STORED"
        new File(folderName + "/test_log_rotation").length mustEqual 16403
        client.get("test_log_rotation") mustEqual v
        new File(folderName + "/test_log_rotation").length mustEqual 0
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
