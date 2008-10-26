/** Copyright 2008 Twitter, Inc. */
package com.twitter.scarling

import java.io.{File, FileInputStream}

import org.specs._


object PersistentQueueSpec extends Specification with TestHelper {

  "PersistentQueue" should {
    "add and remove one item" in {
      withTempFolder {
        val q = new PersistentQueue(folderName, "work")
        q.setup

        q.size mustEqual 0
        q.totalItems mustEqual 0
        q.bytes mustEqual 0
        q.journalSize mustEqual 0

        q.add("hello kitty".getBytes)

        q.size mustEqual 1
        q.totalItems mustEqual 1
        q.bytes mustEqual 15
        q.journalSize mustEqual 20

        new String(q.remove.get) mustEqual "hello kitty"

        q.size mustEqual 0
        q.totalItems mustEqual 1
        q.bytes mustEqual 0
        q.journalSize mustEqual 21

        q.close

        val f = new FileInputStream(new File(folderName, "work"))
        val data = new Array[Byte](21)
        f.read(data)
        data.mkString(":") mustEqual "0:15:0:0:0:0:0:0:0:104:101:108:108:111:32:107:105:116:116:121:1"
      }
    }

    "rotate journals" in {
      withTempFolder {
        val q = new PersistentQueue(folderName, "rolling")
        q.setup
        PersistentQueue.maxJournalSize = 64

        q.add(new Array[Byte](32))
        q.add(new Array[Byte](64))
        q.size mustEqual 2
        q.totalItems mustEqual 2
        q.bytes mustEqual 104
        q.journalSize mustEqual 114
        new File(folderName, "rolling").length mustEqual 114

        q.remove
        q.size mustEqual 1
        q.totalItems mustEqual 2
        q.bytes mustEqual 68
        q.journalSize mustEqual 115
        new File(folderName, "rolling").length mustEqual 115

        // now it should rotate:
        q.remove
        q.size mustEqual 0
        q.totalItems mustEqual 2
        q.bytes mustEqual 0
        q.journalSize mustEqual 0
        new File(folderName, "rolling").length mustEqual 0

        PersistentQueue.maxJournalSize = 16 * 1024 * 1024
      }
    }

    "recover the journal after a restart" in {
      withTempFolder {
        val q = new PersistentQueue(folderName, "rolling")
        q.setup
        q.add("first".getBytes)
        q.add("second".getBytes)
        new String(q.remove.get) mustEqual "first"
        q.journalSize mustEqual 30
        q.close

        val q2 = new PersistentQueue(folderName, "rolling")
        q2.setup
        q2.journalSize mustEqual 30
        new String(q2.remove.get) mustEqual "second"
        q2.journalSize mustEqual 31
        q2.size mustEqual 0
        q2.close

        val q3 = new PersistentQueue(folderName, "rolling")
        q3.setup
        q3.journalSize mustEqual 31
        q3.size mustEqual 0
      }
    }
  }
}
