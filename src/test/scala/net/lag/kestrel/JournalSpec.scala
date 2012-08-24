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
import org.specs.Specification
import com.twitter.logging.TestLogging
import com.twitter.util.{Duration, TempFolder, Time}

class JournalSpec extends Specification with TempFolder with TestLogging with DumpJournal {
  "Journal" should {
    "walk" in {
      withTempFolder {
        val journal = new Journal(folderName + "/a1")
        journal.open()
        journal.add(QItem(Time.now, None, new Array[Byte](32), 0))
        journal.add(QItem(Time.now, None, new Array[Byte](64), 0))
        journal.add(QItem(Time.now, None, new Array[Byte](10), 0))
        journal.close()

        val journal2 = new Journal(folderName + "/a1")
        journal2.walk().map {
          case (item, itemsize) => item match {
            case JournalItem.Add(qitem) => qitem.data.size.toString
            case x => ""
          }
        }.mkString(",") mustEqual "32,64,10"
      }
    }

    "recover from corruption" in {
      withTempFolder {
        val journal = new Journal(folderName + "/a1")
        journal.open()
        journal.add(QItem(Time.now, None, new Array[Byte](32), 0))
        journal.close()

        val f = new FileOutputStream(folderName + "/a1", true)
        f.write(127)
        f.close()

        val journal2 = new Journal(folderName + "/a1")
        journal2.walk().map { case (item, itemsize) => item.toString }.mkString(",") must throwA[BrokenItemException]
      }
    }

    "identify valid queue names" in {
      "simple" in {
        withTempFolder {
          new FileOutputStream(folderName + "/j1").close()
          new FileOutputStream(folderName + "/j2").close()
          Journal.getQueueNamesFromFolder(new File(folderName)) mustEqual Set("j1", "j2")
        }
      }

      "handle queues with archived journals" in {
        withTempFolder {
          new FileOutputStream(folderName + "/j1").close()
          new FileOutputStream(folderName + "/j1.1000").close()
          new FileOutputStream(folderName + "/j1.2000").close()
          new FileOutputStream(folderName + "/j2").close()
          Journal.getQueueNamesFromFolder(new File(folderName)) mustEqual Set("j1", "j2")
        }
      }

      "ignore queues with journals being packed" in {
        withTempFolder {
          new FileOutputStream(folderName + "/j1").close()
          new FileOutputStream(folderName + "/j2").close()
          new FileOutputStream(folderName + "/j2~~").close()
          Journal.getQueueNamesFromFolder(new File(folderName)) mustEqual Set("j1", "j2")
        }
      }

      "ignore subdirectories" in {
        withTempFolder {
          new FileOutputStream(folderName + "/j1").close()
          new FileOutputStream(folderName + "/j2").close()
          new File(folderName, "subdir").mkdirs()
          Journal.getQueueNamesFromFolder(new File(folderName)) mustEqual Set("j1", "j2")
        }
      }
    }

    "identify valid journal files" in {
      "simple" in {
        withTempFolder {
          new FileOutputStream(folderName + "/test.50")
          new FileOutputStream(folderName + "/test.100")
          new FileOutputStream(folderName + "/test.3000")
          new FileOutputStream(folderName + "/test")
          Journal.journalsForQueue(new File(folderName), "test").toList mustEqual
            List("test.50", "test.100", "test.3000", "test")
        }
      }

      "half-finished pack" in {
        withTempFolder {
          new FileOutputStream(folderName + "/test.50")
          new FileOutputStream(folderName + "/test.100")
          new FileOutputStream(folderName + "/test.100.pack")
          new FileOutputStream(folderName + "/test.3000")
          new FileOutputStream(folderName + "/test")
          Journal.journalsForQueue(new File(folderName), "test").toList mustEqual
            List("test.100", "test.3000", "test")

          // and it should clean up after itself:
          new File(folderName + "/test").exists() mustEqual true
          new File(folderName + "/test.100.pack").exists() mustEqual false
          new File(folderName + "/test.100").exists() mustEqual true
          new File(folderName + "/test.50").exists() mustEqual false
        }
      }

      "missing last file" in {
        withTempFolder {
          new FileOutputStream(folderName + "/test.50")
          new FileOutputStream(folderName + "/test.100")
          Journal.journalsForQueue(new File(folderName), "test").toList mustEqual
            List("test.50", "test.100", "test")
        }
      }

      "missing any files" in {
        withTempFolder {
          Journal.journalsForQueue(new File(folderName), "test").toList mustEqual
            List("test")
        }
      }

      "journalsBefore and journalAfter" in {
        withTempFolder {
          new FileOutputStream(folderName + "/test.50")
          new FileOutputStream(folderName + "/test.100")
          new FileOutputStream(folderName + "/test.999")
          new FileOutputStream(folderName + "/test.3000")
          new FileOutputStream(folderName + "/test")

          Journal.journalsBefore(new File(folderName), "test", "test.3000").toList mustEqual
            List("test.50", "test.100", "test.999")
          Journal.journalsBefore(new File(folderName), "test", "test.50").toList mustEqual Nil
          Journal.journalAfter(new File(folderName), "test", "test.100") mustEqual Some("test.999")
          Journal.journalAfter(new File(folderName), "test", "test.3000") mustEqual Some("test")
          Journal.journalAfter(new File(folderName), "test", "test") mustEqual None
        }
      }
    }

    "pack old files" in {
      withTempFolder {
        val journal = new Journal(new File(folderName), "test", null, Duration.MaxValue)
        journal.open()
        journal.add(QItem(Time.now, None, "".getBytes, 0))
        journal.rotate(Nil, false)
        journal.add(QItem(Time.now, None, "".getBytes, 0))
        val checkpoint = journal.rotate(Nil, true)
        val oldFiles = Journal.journalsForQueue(new File(folderName), "test")
        oldFiles.map { f => new File(folderName, f).length }.toList mustEqual List(21, 21, 0)

        journal.startPack(checkpoint.get, Nil, Nil)
        journal.waitForPacksToFinish()

        val files = Journal.journalsForQueue(new File(folderName), "test")
        files.size mustEqual 2
        files mustEqual oldFiles.slice(1, 3)
        dumpJournal("test") mustEqual ""
        files.map { f => new File(folderName, f).length }.toList mustEqual List(0, 0)
      }
    }

    "report file sizes correctly" in {
      withTempFolder {
        val journal = new Journal(new File(folderName), "test", null, Duration.MaxValue)
        journal.open()
        journal.add(QItem(Time.now, None, "".getBytes, 0))
        journal.size mustEqual 21
        journal.archivedSize mustEqual 0

        journal.rotate(Nil, false)
        journal.size mustEqual 0
        journal.archivedSize mustEqual 21

        journal.add(QItem(Time.now, None, "".getBytes, 0))
        journal.size mustEqual 21
        journal.archivedSize mustEqual 21

        journal.rewrite(Nil, Nil)
        journal.size mustEqual 0
        journal.archivedSize mustEqual 0
      }
    }

    "rebuild from a checkpoint correctly" in {
      withTempFolder {
        val journal = new Journal(new File(folderName), "test", null, Duration.MaxValue)
        journal.open()

        val initialOpenItems = List(
          QItem(Time.now, None, "A".getBytes, 6),
          QItem(Time.now, None, "B".getBytes, 7),
          QItem(Time.now, None, "C".getBytes, 8)
        )
        val queue1 = List(
          QItem(Time.now, None, "D".getBytes, 0),
          QItem(Time.now, None, "E".getBytes, 0),
          QItem(Time.now, None, "F".getBytes, 0)
        )
        journal.rewrite(initialOpenItems, queue1)
        dumpJournal("test") mustEqual
          "add(1:0:A), remove-tentative(6), add(1:0:B), remove-tentative(7), add(1:0:C), remove-tentative(8), " +
          "add(1:0:D), add(1:0:E), add(1:0:F)"

        val checkpoint = journal.rotate(initialOpenItems, true)
        journal.remove() // there goes D
        journal.removeTentative(9) // E
        journal.confirmRemove(6) // A
        journal.add(QItem(Time.now, None, "G".getBytes, 0))

        val newOpenItems = initialOpenItems.drop(1) ++ List(QItem(Time.now, None, "E".getBytes, 9)) // B, C, E
        val queue2 = List(QItem(Time.now, None, "F".getBytes, 0))
        journal.startPack(checkpoint.get, newOpenItems, queue2)
        journal.waitForPacksToFinish()

        dumpJournal("test") mustEqual
          "add(1:0:A), remove-tentative(6), add(1:0:B), remove-tentative(7), add(1:0:C), remove-tentative(8), " +
          "add(1:0:E), add(1:0:F), " +
          "remove, remove-tentative(9), confirm-remove(6), add(1:0:G)"
      }
    }
  }
}
