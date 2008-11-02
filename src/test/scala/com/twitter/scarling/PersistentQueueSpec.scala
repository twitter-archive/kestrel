/** Copyright 2008 Twitter, Inc. */
package com.twitter.scarling

import java.io.{File, FileInputStream}
import net.lag.configgy.Config
import org.specs._


object PersistentQueueSpec extends Specification with TestHelper {

  "PersistentQueue" should {
    "add and remove one item" in {
      withTempFolder {
        val q = new PersistentQueue(folderName, "work", Config.fromMap(Map.empty))
        q.setup

        q.length mustEqual 0
        q.totalItems mustEqual 0
        q.bytes mustEqual 0
        q.journalSize mustEqual 0

        q.add("hello kitty".getBytes)

        q.length mustEqual 1
        q.totalItems mustEqual 1
        q.bytes mustEqual 11
        q.journalSize mustEqual 32

        new String(q.remove.get) mustEqual "hello kitty"

        q.length mustEqual 0
        q.totalItems mustEqual 1
        q.bytes mustEqual 0
        q.journalSize mustEqual 33

        q.close

        val f = new FileInputStream(new File(folderName, "work"))
        val data = new Array[Byte](33)
        f.read(data)
        for (i <- 5 until 13) data(i) = 3
        data.mkString(":") mustEqual "2:27:0:0:0:3:3:3:3:3:3:3:3:0:0:0:0:0:0:0:0:104:101:108:108:111:32:107:105:116:116:121:1"
      }
    }

    "rotate journals" in {
      withTempFolder {
        val q = new PersistentQueue(folderName, "rolling", Config.fromMap(Map.empty))
        q.setup
        PersistentQueue.maxJournalSize = 64

        q.add(new Array[Byte](32))
        q.add(new Array[Byte](64))
        q.length mustEqual 2
        q.totalItems mustEqual 2
        q.bytes mustEqual 32 + 64
        q.journalSize mustEqual 32 + 64 + 16 + 16 + 5 + 5
        new File(folderName, "rolling").length mustEqual 32 + 64 + 16 + 16 + 5 + 5

        q.remove
        q.length mustEqual 1
        q.totalItems mustEqual 2
        q.bytes mustEqual 64
        q.journalSize mustEqual 32 + 64 + 16 + 16 + 5 + 5 + 1
        new File(folderName, "rolling").length mustEqual 32 + 64 + 16 + 16 + 5 + 5 + 1

        // now it should rotate:
        q.remove
        q.length mustEqual 0
        q.totalItems mustEqual 2
        q.bytes mustEqual 0
        q.journalSize mustEqual 0
        new File(folderName, "rolling").length mustEqual 0

        PersistentQueue.maxJournalSize = 16 * 1024 * 1024
      }
    }

    "recover the journal after a restart" in {
      withTempFolder {
        val q = new PersistentQueue(folderName, "rolling", Config.fromMap(Map.empty))
        q.setup
        q.add("first".getBytes)
        q.add("second".getBytes)
        new String(q.remove.get) mustEqual "first"
        q.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1
        q.close

        val q2 = new PersistentQueue(folderName, "rolling", Config.fromMap(Map.empty))
        q2.setup
        q2.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1
        new String(q2.remove.get) mustEqual "second"
        q2.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1 + 1
        q2.length mustEqual 0
        q2.close

        val q3 = new PersistentQueue(folderName, "rolling", Config.fromMap(Map.empty))
        q3.setup
        q3.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1 + 1
        q3.length mustEqual 0
      }
    }

    "honor max_items" in {
      withTempFolder {
        val q = new PersistentQueue(folderName, "weather_updates", Config.fromMap(Map("max_items" -> "1")))
        q.setup
        q.add("sunny".getBytes) mustEqual true
        q.add("rainy".getBytes) mustEqual false
        q.length mustEqual 1
      }
    }

    "honor max_age" in {
      withTempFolder {
        val config = Config.fromMap(Map("max_age" -> "1"))
        val q = new PersistentQueue(folderName, "weather_updates", config)
        q.setup
        q.add("sunny".getBytes) mustEqual true
        q.length mustEqual 1
        Thread.sleep(1000)
        q.remove mustEqual None

        config("max_age") = 60
        q.add("rainy".getBytes) mustEqual true
        config("max_age") = 1
        Thread.sleep(1000)
        q.remove mustEqual None
      }
    }
  }
}
