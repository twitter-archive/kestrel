package com.twitter.scarling

import java.io.{File}
import java.net.Socket
import scala.collection.Map
import net.lag.configgy.Config
import net.lag.logging.Logger
import sorg.testing._


object ServerTests extends Tests {

    override def testName = "ServerTests"

    private var folder: File = null


    override def setUp = {
        // make a temporary folder for this test's queue files & logs
        do {
            folder = new File("./scarling-test-" + System.currentTimeMillis)
        } while (! folder.mkdir)

        val config = new Config
        config("host") = "localhost"
        config("port") = 22122
        config("queue_path") = folder.getCanonicalPath
        config("log.console") = true
        config("log.level") = "debug"
        config("log.filename") = "/tmp/foo"
        //Logger.configure(config.getAttributes("log").get, false, false)

        Scarling.startup(config)
        Scarling.queues.queue("test_log_rotation").get.maxJournalSize = 16 * 1024
    }

    override def tearDown = {
        Scarling.shutdown

        for (val f <- folder.listFiles) {
            f.delete
        }
        folder.delete
        Thread.sleep(1000)
    }


    test("set and get one entry") {
        val v = (Math.random * 0x7fffffff).toInt
        val client = new TestClient("localhost", 22122)
        expect("") { client.get("test_one_entry") }
        expect("STORED") { client.set("test_one_entry", v.toString) }
        expect(v.toString) { client.get("test_one_entry") }
        expect("") { client.get("test_one_entry") }
    }

    test("set with expiry") {
        val v = (Math.random * 0x7fffffff).toInt
        val client = new TestClient("localhost", 22122)
        expect("") { client.get("test_set_with_expiry") }
        expect("STORED") { client.set("test_set_with_epxiry", (v + 2).toString, (System.currentTimeMillis / 1000).toInt) }
        expect("STORED") { client.set("test_set_with_expiry", v.toString) }
        Thread.sleep(1000)
        expect(v.toString) { client.get("test_set_with_expiry") }
    }

    test("age") {
        val client = new TestClient("localhost", 22122)
        expect("STORED") { client.set("test_age", "nibbler") }
        Thread.sleep(1000)
        expect("nibbler") { client.get("test_age") }
        expect(true) { client.stats.contains("queue_test_age_age") }
        expect(true) { client.stats("queue_test_age_age").toInt >= 1000 }
    }

    test("log rotation") {
        var v = "x"
        for (val i <- 1.to(13)) { v = v + v }    // 8192

        val client = new TestClient("localhost", 22122)

        expect("STORED") { client.set("test_log_rotation", v) }
        expect(8201) { new File(folder + "/test_log_rotation").length }
        expect(v) { client.get("test_log_rotation") }
        expect(8202) { new File(folder + "/test_log_rotation").length }

        expect("") { client.get("test_log_rotation") }
        expect(8202) { new File(folder + "/test_log_rotation").length }

        expect("STORED") { client.set("test_log_rotation", v) }
        expect(16403) { new File(folder + "/test_log_rotation").length }
        expect(v) { client.get("test_log_rotation") }
        expect(0) { new File(folder + "/test_log_rotation").length }
        expect(1) { folder.list.length }
    }

    test("stats") {
        val client = new TestClient("localhost", 22122)
        val stats = client.stats
        val basicStats = Array("bytes", "time", "limit_maxbytes", "cmd_get", "version",
                               "bytes_written", "cmd_set", "get_misses", "total_connections",
                               "curr_connections", "curr_items", "uptime", "get_hits", "total_items",
                               "bytes_read")
        for (val key <- basicStats) { expect(true) { stats contains key } }
    }

    test("unknown command returns valid response") {
        expect("CLIENT_ERROR") { new TestClient("localhost", 22122).add("cheese", "swiss") }
    }

    test("disconnect and reconnect is okay") {
        val v = (Math.random * 0x7fffffff).toInt
        val client = new TestClient("localhost", 22122)
        client.set("disconnecting", v.toString)
        client.disconnect
        client.connect
        expect(v.toString) { client.get("disconnecting") }
    }
}
