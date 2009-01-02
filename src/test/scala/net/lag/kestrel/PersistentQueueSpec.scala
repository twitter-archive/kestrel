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

package net.lag.kestrel

import java.io.{File, FileInputStream}
import java.util.concurrent.CountDownLatch
import scala.actors.Actor.actor
import net.lag.configgy.Config
import org.specs._


object PersistentQueueSpec extends Specification with TestHelper {

  def dumpJournal(folderName: String, qname: String): String = {
    var rv = List[JournalItem]()
    new Journal(new File(folderName, qname).getCanonicalPath).replay(qname) { item => rv = item :: rv }
    rv.reverse map {
      case JournalItem.Add(item) => "add(%s)".format(new String(item.data))
      case JournalItem.Remove => "remove"
      case JournalItem.RemoveTentative => "remove-tentative"
      case JournalItem.SavedXid(xid) => "xid(%d)".format(xid)
      case JournalItem.Unremove(xid) => "unremove(%d)".format(xid)
      case JournalItem.ConfirmRemove(xid) => "confirm-remove(%d)".format(xid)
    } mkString ", "
  }


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

        new String(q.remove.get.data) mustEqual "hello kitty"

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

    "flush all items" in {
      withTempFolder {
        val q = new PersistentQueue(folderName, "work", Config.fromMap(Map.empty))
        q.setup

        q.length mustEqual 0
        q.totalItems mustEqual 0
        q.bytes mustEqual 0
        q.journalSize mustEqual 0

        q.add("alpha".getBytes)
        q.add("beta".getBytes)
        q.add("gamma".getBytes)
        q.length mustEqual 3

        q.flush
        q.length mustEqual 0

        // journal should contain exactly: one unfinished transaction, 2 items.
        q.close
        var jlist: List[JournalItem] = Nil
        val j = new Journal(new File(folderName, "work").getCanonicalPath)
        j.replay("things") { j => jlist = jlist ++ List(j) }
        jlist.size mustEqual 6
        jlist(0).asInstanceOf[JournalItem.Add].item.data.size mustEqual 5
        jlist(1).asInstanceOf[JournalItem.Add].item.data.size mustEqual 4
        jlist(2).asInstanceOf[JournalItem.Add].item.data.size mustEqual 5
        jlist(3) mustEqual JournalItem.Remove
        jlist(4) mustEqual JournalItem.Remove
        jlist(5) mustEqual JournalItem.Remove
      }
    }

    "rotate journals" in {
      withTempFolder {
        val q = new PersistentQueue(folderName, "rolling", Config.fromMap(Map.empty))
        q.setup
        q._maxJournalSize = Some(64)

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
        q.journalSize mustEqual 5   // saved xid.
        new File(folderName, "rolling").length mustEqual 5
      }
    }

    "recover the journal after a restart" in {
      withTempFolder {
        val q = new PersistentQueue(folderName, "rolling", Config.fromMap(Map.empty))
        q.setup
        q.add("first".getBytes)
        q.add("second".getBytes)
        new String(q.remove.get.data) mustEqual "first"
        q.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1
        q.close

        val q2 = new PersistentQueue(folderName, "rolling", Config.fromMap(Map.empty))
        q2.setup
        q2.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1
        new String(q2.remove.get.data) mustEqual "second"
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
        val config = Config.fromMap(Map("max_age" -> "3"))
        val q = new PersistentQueue(folderName, "weather_updates", config)
        q.setup
        q.add("sunny".getBytes) mustEqual true
        q.length mustEqual 1
        Time.advance(3000)
        q.remove mustEqual None

        config("max_age") = 60
        q.add("rainy".getBytes) mustEqual true
        config("max_age") = 1
        Time.advance(5000)
        q.remove mustEqual None
      }
    }

    "allow max_journal_size and max_memory_size to be overridden per queue" in {
      withTempFolder {
        val config1 = Config.fromMap(Map("max_memory_size" -> "123"))
        val config2 = Config.fromMap(Map("max_journal_size" -> "123"))
        val q1 = new PersistentQueue(folderName, "test1", config1)
        q1.maxJournalSize mustEqual PersistentQueue.maxJournalSize
        q1.maxMemorySize mustEqual 123
        val q2 = new PersistentQueue(folderName, "test2", config2)
        q2.maxJournalSize mustEqual 123
        q2.maxMemorySize mustEqual PersistentQueue.maxMemorySize
      }
    }

    "drop into read-behind mode" in {
      withTempFolder {
        PersistentQueue.maxMemorySize = 1024
        val q = new PersistentQueue(folderName, "things", Config.fromMap(Map.empty))
        q.setup
        for (i <- 0 until 10) {
          val data = new Array[Byte](128)
          data(0) = i.toByte
          q.add(data)
          q.inReadBehind mustEqual (i >= 8)
        }
        q.inReadBehind mustBe true
        q.length mustEqual 10
        q.bytes mustEqual 1280
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024

        // read 1 item. queue should pro-actively read the next item in from disk.
        val d0 = q.remove.get.data
        d0(0) mustEqual 0
        q.inReadBehind mustBe true
        q.length mustEqual 9
        q.bytes mustEqual 1152
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024

        // adding a new item should be ok
        val w10 = new Array[Byte](128)
        w10(0) = 10.toByte
        q.add(w10)
        q.inReadBehind mustBe true
        q.length mustEqual 10
        q.bytes mustEqual 1280
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024

        // read again.
        val d1 = q.remove.get.data
        d1(0) mustEqual 1
        q.inReadBehind mustBe true
        q.length mustEqual 9
        q.bytes mustEqual 1152
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024

        // and again.
        val d2 = q.remove.get.data
        d2(0) mustEqual 2
        q.inReadBehind mustBe true
        q.length mustEqual 8
        q.bytes mustEqual 1024
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024

        for (i <- 3 until 11) {
          val d = q.remove.get.data
          d(0) mustEqual i
          q.inReadBehind mustBe false
          q.length mustEqual 10 - i
          q.bytes mustEqual 128 * (10 - i)
          q.memoryLength mustEqual 10 - i
          q.memoryBytes mustEqual 128 * (10 - i)
        }
      }
    }

    "drop into read-behind mode on startup" in {
      withTempFolder {
        PersistentQueue.maxMemorySize = 1024
        val q = new PersistentQueue(folderName, "things", Config.fromMap(Map.empty))
        q.setup
        for (i <- 0 until 10) {
          val data = new Array[Byte](128)
          data(0) = i.toByte
          q.add(data)
          q.inReadBehind mustEqual (i >= 8)
        }
        q.inReadBehind mustBe true
        q.length mustEqual 10
        q.bytes mustEqual 1280
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024
        q.close

        val q2 = new PersistentQueue(folderName, "things", Config.fromMap(Map.empty))
        q2.setup

        q2.inReadBehind mustBe true
        q2.length mustEqual 10
        q2.bytes mustEqual 1280
        q2.memoryLength mustEqual 8
        q2.memoryBytes mustEqual 1024

        for (i <- 0 until 10) {
          val d = q2.remove.get.data
          d(0) mustEqual i
          q2.inReadBehind mustEqual (i < 2)
          q2.length mustEqual 9 - i
          q2.bytes mustEqual 128 * (9 - i)
          q2.memoryLength mustEqual (if (i < 2) 8 else 9 - i)
          q2.memoryBytes mustEqual (if (i < 2) 1024 else 128 * (9 - i))
        }
      }
    }

    "drop into read-behind mode during journal processing, then return to ordinary times" in {
      withTempFolder {
        PersistentQueue.maxMemorySize = 1024
        val q = new PersistentQueue(folderName, "things", Config.fromMap(Map.empty))
        q.setup
        for (i <- 0 until 10) {
          val data = new Array[Byte](128)
          data(0) = i.toByte
          q.add(data)
          q.inReadBehind mustEqual (i >= 8)
        }
        q.inReadBehind mustBe true
        q.length mustEqual 10
        q.bytes mustEqual 1280
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024
        for (i <- 0 until 10) {
          q.remove
        }
        q.inReadBehind mustBe false
        q.length mustEqual 0
        q.bytes mustEqual 0
        q.memoryLength mustEqual 0
        q.memoryBytes mustEqual 0
        q.close

        val q2 = new PersistentQueue(folderName, "things", Config.fromMap(Map.empty))
        q2.setup
        q2.inReadBehind mustBe false
        q2.length mustEqual 0
        q2.bytes mustEqual 0
        q2.memoryLength mustEqual 0
        q2.memoryBytes mustEqual 0
      }
    }

    "handle timeout reads" in {
      withTempFolder {
        PersistentQueue.maxMemorySize = 1024
        val q = new PersistentQueue(folderName, "things", Config.fromMap(Map.empty))
        q.setup

        actor {
          Thread.sleep(100)
          q.add("hello".getBytes)
        }

        var rv: String = null
        val latch = new CountDownLatch(1)
        actor {
          q.remove(Time.now + 250, false) { item =>
            rv = new String(item.get.data)
            latch.countDown
          }
        }
        latch.await
        rv mustEqual "hello"
      }
    }

    "correctly interleave transactions in the journal" in {
      withTempFolder {
        PersistentQueue.maxMemorySize = 1024
        val q = new PersistentQueue(folderName, "things", Config.fromMap(Map.empty))
        q.setup
        q.add("house".getBytes)
        q.add("cat".getBytes)
        q.journalSize mustEqual 2 * 21 + 8

        val house = q.remove(true).get
        new String(house.data) mustEqual "house"
        house.xid mustEqual 1
        q.journalSize mustEqual 2 * 21 + 8 + 1

        val cat = q.remove(true).get
        new String(cat.data) mustEqual "cat"
        cat.xid mustEqual 2
        q.journalSize mustEqual 2 * 21 + 8 + 1 + 1

        q.unremove(house.xid)
        q.journalSize mustEqual 2 * 21 + 8 + 1 + 1 + 5

        q.confirmRemove(cat.xid)
        q.journalSize mustEqual 2 * 21 + 8 + 1 + 1 + 5 + 5
        q.length mustEqual 1
        q.bytes mustEqual 5

        new String(q.remove.get.data) mustEqual "house"
        q.length mustEqual 0
        q.bytes mustEqual 0

        q.close
        dumpJournal(folderName, "things") mustEqual
          "add(house), add(cat), remove-tentative, remove-tentative, unremove(1), confirm-remove(2), remove"

        // and journal is replayed correctly.
        val q2 = new PersistentQueue(folderName, "things", Config.fromMap(Map.empty))
        q2.setup
        q2.length mustEqual 0
        q2.bytes mustEqual 0
      }
    }

    "recover a journal with open transactions" in {
      withTempFolder {
        val q = new PersistentQueue(folderName, "things", Config.fromMap(Map.empty))
        q.setup
        q.add("one".getBytes)
        q.add("two".getBytes)
        q.add("three".getBytes)
        q.add("four".getBytes)
        q.add("five".getBytes)

        val item1 = q.remove(true)
        item1 must beSome[QItem].which { item => new String(item.data) == "one" }
        new String(item1.get.data) mustEqual "one"
        val item2 = q.remove(true)
        new String(item2.get.data) mustEqual "two"
        val item3 = q.remove(true)
        new String(item3.get.data) mustEqual "three"
        val item4 = q.remove(true)
        new String(item4.get.data) mustEqual "four"

        q.confirmRemove(item2.get.xid)
        q.confirmRemove(item4.get.xid)
        q.close

        val q2 = new PersistentQueue(folderName, "things", Config.fromMap(Map.empty))
        q2.setup
        q2.length mustEqual 3
        q2.openTransactionCount mustEqual 0
        new String(q2.remove.get.data) mustEqual "one"
        new String(q2.remove.get.data) mustEqual "three"
        new String(q2.remove.get.data) mustEqual "five"
        q2.length mustEqual 0
      }
    }

    "recreate the journal file when it gets too big" in {
      withTempFolder {
        val config = Config.fromMap(Map("max_journal_size" -> "1024",
                                        "max_journal_overflow" -> "3"))
        val q = new PersistentQueue(folderName, "things", config)
        q.setup
        q.add(new Array[Byte](512))
        // can't roll the journal normally, cuz there's always one item left.
        for (i <- 0 until 5) {
          q.add(new Array[Byte](512))
          // last remove will be an incomplete transaction:
          q.remove(i == 4) must beSome[QItem].which { item => item.data.size == 512 }
        }
        q.length mustEqual 1
        q.journalSize mustEqual (512 * 6) + (6 * 21) + 5

        // next add should force a recreate.
        q.add(new Array[Byte](512))
        q.length mustEqual 2
        q.journalSize mustEqual ((512 + 16) * 3) + 9 + 1 + 5 + (5 * 2)

        // journal should contain exactly: one unfinished transaction, 2 items.
        q.close
        var jlist: List[JournalItem] = Nil
        val j = new Journal(new File(folderName, "things").getCanonicalPath)
        j.replay("things") { j => jlist = jlist ++ List(j) }
        jlist(0).asInstanceOf[JournalItem.Add].item.xid mustEqual 1
        jlist(0).asInstanceOf[JournalItem.Add].item.data.size mustEqual 512
        jlist(1) mustBe JournalItem.RemoveTentative
        jlist(2).asInstanceOf[JournalItem.SavedXid].xid mustEqual 1
        jlist(3).asInstanceOf[JournalItem.Add].item.data.size mustEqual 512
        jlist(4).asInstanceOf[JournalItem.Add].item.data.size mustEqual 512
      }
    }
  }
}
