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

import java.io.{File, FileInputStream}
import java.util.concurrent.CountDownLatch
import scala.actors.Actor.actor
import scala.collection.mutable
import net.lag.configgy.Config
import org.specs._
import org.specs.matcher.Matcher


object PersistentQueueSpec extends Specification with TestHelper {

  def dumpJournal(qname: String): String = {
    var rv = new mutable.ListBuffer[JournalItem]
    new Journal(new File(folderName, qname).getCanonicalPath, Config.fromMap(Map.empty)).replay(qname) { item => rv += item }
    rv map {
      case JournalItem.Add(item) =>
        if (item.data.size > 0 && item.data(0) > 0) {
          "add(%d:%d:%s)".format(item.data.size, item.xid, new String(item.data))
        } else {
          "add(%d:%d)".format(item.data.size, item.xid)
        }
      case JournalItem.Remove => "remove"
      case JournalItem.RemoveTentative => "remove-tentative"
      case JournalItem.SavedXid(xid) => "xid(%d)".format(xid)
      case JournalItem.Unremove(xid) => "unremove(%d)".format(xid)
      case JournalItem.ConfirmRemove(xid) => "confirm-remove(%d)".format(xid)
    } mkString ", "
  }

  // convenience
  def makeQueue(name: String, options: (String, String)*) = {
    new PersistentQueue(folderName, name, Config.fromMap(Map(options: _*)))
  }

  def beSomeQItem(s: String) = new Matcher[Option[QItem]] {
    def apply(qitemEval: => Option[QItem]) = {
      val qitem = qitemEval
      (qitem.isDefined && (new String(qitem.get.data) == s), "ok", "wrong or missing queue item")
    }
  }

  def beSomeQItem(len: Int) = new Matcher[Option[QItem]] {
    def apply(qitemEval: => Option[QItem]) = {
      val qitem = qitemEval
      (qitem.isDefined && (qitem.get.data.size == len), "ok", "wrong or missing queue item")
    }
  }

  def withMaxMemorySize(newLimit: Long)(f: => Unit) = {
    val oldLimit = PersistentQueue.maxMemorySize
    PersistentQueue.maxMemorySize = newLimit
    try {
      f
    } finally {
      PersistentQueue.maxMemorySize = oldLimit
    }
  }

  def put(q: PersistentQueue, bytes: Int, n: Int) {
    val data = new Array[Byte](bytes)
    data(0) = n.toByte
    q.add(data)
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
        dumpJournal("work") mustEqual "add(11:0:hello kitty), remove"
      }
    }

    "resist adding an item that's too large" in {
      withTempFolder {
        val q = new PersistentQueue(folderName, "work", Config.fromMap(Map("max_item_size" -> "128")))
        q.setup
        q.length mustEqual 0
        q.add(new Array[Byte](127)) mustEqual true
        q.add(new Array[Byte](128)) mustEqual true
        q.add(new Array[Byte](129)) mustEqual false
        q.close
      }
    }

    "flush all items" in {
      withTempFolder {
        val q = makeQueue("work")
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
        dumpJournal("work") mustEqual
          "add(5:0:alpha), add(4:0:beta), add(5:0:gamma), remove, remove, remove"
      }
    }

    "rotate journals" in {
      withTempFolder {
        val q = makeQueue("rolling")
        q.setup
        q.maxJournalSize set Some(64)

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
        val q = makeQueue("rolling")
        q.setup
        q.add("first".getBytes)
        q.add("second".getBytes)
        new String(q.remove.get.data) mustEqual "first"
        q.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1
        q.close

        val q2 = makeQueue("rolling")
        q2.setup
        q2.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1
        new String(q2.remove.get.data) mustEqual "second"
        q2.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1 + 1
        q2.length mustEqual 0
        q2.close

        val q3 = makeQueue("rolling")
        q3.setup
        q3.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1 + 1
        q3.length mustEqual 0
      }
    }

    "honor max_age" in {
      withTempFolder {
        val q = makeQueue("weather_updates", "max_age" -> "3")
        q.setup
        q.add("sunny".getBytes) mustEqual true
        q.length mustEqual 1
        Time.advance(3000)
        q.remove mustEqual None

        q.config("max_age") = 60
        q.add("rainy".getBytes) mustEqual true
        q.config("max_age") = 1
        Time.advance(5000)
        q.remove mustEqual None
      }
    }

    "allow max_journal_size and max_memory_size to be overridden per queue" in {
      withTempFolder {
        val q1 = makeQueue("test1", "max_memory_size" -> "123")
        q1.maxJournalSize() mustEqual PersistentQueue.maxJournalSize
        q1.maxMemorySize() mustEqual 123
        val q2 = makeQueue("test2", "max_journal_size" -> "123")
        q2.maxJournalSize() mustEqual 123
        q2.maxMemorySize() mustEqual PersistentQueue.maxMemorySize
      }
    }

    "drop into read-behind mode" in {
      "on insert" in {
        withTempFolder {
          withMaxMemorySize(1024) {
            val q = makeQueue("things")
            q.setup
            for (i <- 0 until 10) {
              put(q, 128, i)
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
            put(q, 128, 10)
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
      }

      "on startup" in {
        withTempFolder {
          withMaxMemorySize(1024) {
            val q = makeQueue("things")
            q.setup
            for (i <- 0 until 10) {
              put(q, 128, i)
              q.inReadBehind mustEqual (i >= 8)
            }
            q.inReadBehind mustBe true
            q.length mustEqual 10
            q.bytes mustEqual 1280
            q.memoryLength mustEqual 8
            q.memoryBytes mustEqual 1024
            q.close

            val q2 = makeQueue("things")
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
      }

      "during journal processing, then return to ordinary times" in {
        withTempFolder {
          withMaxMemorySize(1024) {
            val q = makeQueue("things")
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

            val q2 = makeQueue("things")
            q2.setup
            q2.inReadBehind mustBe false
            q2.length mustEqual 0
            q2.bytes mustEqual 0
            q2.memoryLength mustEqual 0
            q2.memoryBytes mustEqual 0
          }
        }
      }
    }

    "handle timeout reads" in {
      withTempFolder {
        withMaxMemorySize(1024) {
          val q = makeQueue("things")
          q.setup

          actor {
            Thread.sleep(100)
            q.add("hello".getBytes)
          }

          var rv: String = null
          val latch = new CountDownLatch(1)
          actor {
            q.removeReact(Time.now + 250, false) { item =>
              rv = new String(item.get.data)
              latch.countDown
            }
          }
          latch.await
          rv mustEqual "hello"
        }
      }
    }

    "correctly interleave transactions in the journal" in {
      withTempFolder {
        withMaxMemorySize(1024) {
          val q = makeQueue("things")
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
          dumpJournal("things") mustEqual
            "add(5:0:house), add(3:0:cat), remove-tentative, remove-tentative, unremove(1), confirm-remove(2), remove"

          // and journal is replayed correctly.
          val q2 = makeQueue("things")
          q2.setup
          q2.length mustEqual 0
          q2.bytes mustEqual 0
        }
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
        val q = makeQueue("things", "max_journal_size" -> "1024", "max_journal_overflow" -> "3")
        q.setup
        q.add(new Array[Byte](512))
        // can't roll the journal normally, cuz there's always one item left.
        for (i <- 0 until 5) {
          q.add(new Array[Byte](512))
          // last remove will be an incomplete transaction:
          q.remove(i == 4) must beSomeQItem(512)
        }
        q.length mustEqual 2
        q.journalSize mustEqual (512 * 6) + (6 * 21) + 5

        // next add should force a recreate.
        q.add(new Array[Byte](512))
        q.length mustEqual 3
        q.journalSize mustEqual ((512 + 16) * 3) + 9 + 1 + 5 + (5 * 2)

        // journal should contain exactly: one unfinished transaction, 2 items.
        q.close
        dumpJournal("things") mustEqual "add(512:1), remove-tentative, xid(1), add(512:0), add(512:0)"
      }
    }

    "don't recreate the journal file if the queue itself is still huge" in {
      withTempFolder {
        val q = makeQueue("things", "max_journal_size" -> "1024", "max_journal_overflow" -> "3")
        q.setup
        for (i <- 0 until 8) {
          q.add(new Array[Byte](512))
        }
        q.length mustEqual 8
        q.bytes mustEqual 4096
        dumpJournal("things").contains("xid") mustBe false
        q.journalSize mustEqual (512 + 21) * 8
      }
    }

    "report an age of zero on an empty queue" in {
      withTempFolder {
        val q = makeQueue("things")
        q.setup
        put(q, 128, 0)
        Thread.sleep(10)
        q.remove() must beSomeQItem(128)
        q.length mustEqual 0
        q.currentAge mustEqual 0
      }
    }
  }


  "PersistentQueue with no journal" should {
    "create no journal" in {
      withTempFolder {
        val q = makeQueue("mem", "journal" -> "off")
        q.setup

        q.add("coffee".getBytes)
        new File(folderName, "mem").exists mustBe false
        q.remove must beSomeQItem("coffee")
      }
    }

    "lose all data after being destroyed" in {
      withTempFolder {
        val q = makeQueue("mem", "journal" -> "off")
        q.setup
        q.add("coffee".getBytes)
        q.close

        val q2 = makeQueue("mem", "journal" -> "off")
        q2.setup
        q2.remove mustEqual None
      }
    }
  }


  "PersistentQueue with item/size limit" should {
    "honor max_items" in {
      withTempFolder {
        val q = makeQueue("weather_updates", "max_items" -> "1")
        q.setup
        q.add("sunny".getBytes) mustEqual true
        q.add("rainy".getBytes) mustEqual false
        q.length mustEqual 1
        q.remove must beSomeQItem("sunny")
      }
    }

    "honor max_size" in {
      withTempFolder {
        val q = makeQueue("weather_updates", "max_size" -> "510")
        q.setup
        q.add(("a" * 256).getBytes) mustEqual true
        q.add(("b" * 256).getBytes) mustEqual true
        q.add("television".getBytes) mustEqual false
        q.length mustEqual 2
        q.bytes mustEqual 512
        q.remove must beSomeQItem("a" * 256)
      }
    }

    "drop older items when discard_old_when_full is set" in {
      withTempFolder {
        val q = makeQueue("weather_updates", "max_items" -> "3", "discard_old_when_full" -> "true")
        q.setup
        q.add("sunny".getBytes) mustEqual true
        q.add("rainy".getBytes) mustEqual true
        q.add("cloudy".getBytes) mustEqual true
        q.add("snowy".getBytes) mustEqual true
        q.length mustEqual 3
        q.remove must beSomeQItem("rainy")
      }
    }
  }
}
