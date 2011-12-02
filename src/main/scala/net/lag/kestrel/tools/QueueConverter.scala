/*
 * Copyright 2011 Twitter, Inc.
 * Copyright 2011 Robey Pointer <robeypointer@gmail.com>
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
package tools

import com.twitter.conversions.storage._
import com.twitter.conversions.string._
import com.twitter.libkestrel
import com.twitter.util.Duration
import java.io.{File, FileNotFoundException, IOException}
import java.security.MessageDigest
import net.lag.kestrel.oldjournal
import scala.collection.mutable

object QueueConverter {
  var oldFolder: File = null
  var newFolder: File = null
  var queueName: Option[String] = None
  var allowDupes = false

  def usage() {
    println()
    println("usage: qconverter.sh <old_folder> <new_folder>")
    println("    convert old (2.x) kestrel journal file(s) into the 3.0 format.")
    println("    journals are read from <old_folder> and written to <new_folder>.")
    println()
    println("options:")
    println("    -q name         only convert one queue")
    println("    -D              allow duplicate queue items")
    println("        (by default, queue items are unique-ified)")
    println()
  }

  def parseArgs(args: List[String]): Unit = args match {
    case Nil =>
    case "--help" :: xs =>
      usage()
      System.exit(0)
    case "-q" :: name :: xs =>
      queueName = Some(name)
      parseArgs(xs)
    case "-D" :: xs =>
      allowDupes = true
      parseArgs(xs)
    case a :: b :: xs =>
      oldFolder = new File(a)
      newFolder = new File(b)
      parseArgs(xs)
  }

  def convertQueue(name: String, fanouts: Seq[String]) {
    val md5 = MessageDigest.getInstance("MD5")
    val seenIds = new mutable.HashMap[String, Long]

    val newJournal = new libkestrel.Journal(newFolder, name, 16.megabytes, null, Duration.MaxValue,
      None)
    var lastUpdate = 0L
    var dupes = 0
    var items = 0

    def addItem(item: oldjournal.QItem): String = {
      val hash = md5.digest(item.data).hexlify
      if (allowDupes || !(seenIds contains hash)) {
        val qitem = newJournal.put(item.data, item.addTime, item.expiry).map {
          case (qitem, sync) => qitem
        }.get()
        seenIds(hash) = qitem.id

        val size = newJournal.journalSize
        if (size - lastUpdate > 1024 * 1024 || lastUpdate == 0L) {
          print("\rQueue %s: write %-6s    ".format(name, size.bytes.toHuman))
          Console.flush()
          lastUpdate = size
        }
        items += 1
      } else {
        dupes += 1
      }
      hash
    }

    def stateFor(queueName: String): Seq[oldjournal.QItem] = {
      val files = oldjournal.Journal.journalsForQueue(oldFolder, queueName).map { filename =>
        new File(oldFolder, filename).getCanonicalPath
      }
      val packer = new oldjournal.JournalPacker(files)
      val journalState = packer { (bytes1, bytes2) =>
        print("\rQueue %s: read %-6s    ".format(queueName, bytes2.bytes.toHuman))
        Console.flush()
      }
      (journalState.openTransactions ++ journalState.items)
    }

    // add items from the "parent" queue.
    stateFor(name).foreach { item => addItem(item) }

    // for each fanout queue, add items that weren't in the parent queue.
    val fanoutUnseenIds = new mutable.HashMap[String, mutable.Set[String]]
    fanouts.foreach { fanoutName =>
      fanoutUnseenIds(fanoutName) = new mutable.HashSet[String]
      stateFor(name + "+" + fanoutName).foreach { item =>
        fanoutUnseenIds(fanoutName) += addItem(item)
      }
    }

    /*
     * for each fanout queue, take all the items that are in the "parent" queue, but were NOT in
     * this queue, and mark them as "read".
     */
    fanouts.foreach { fanoutName =>
      val reader = newJournal.reader(fanoutName)
      reader.head = 0L
      (seenIds.keySet -- fanoutUnseenIds(fanoutName)).foreach { hash =>
        reader.commit(seenIds(hash))
      }
      reader.checkpoint().get()
    }

    print("\r" + (" " * 70) + "\r")
    println("Queue %s:\n%6d items, %8s, %6d dupes discarded".format(
      name, items, newJournal.journalSize.bytes.toHuman, dupes))
    fanouts.foreach { fanoutName =>
      val reader = newJournal.reader(fanoutName)
      println("    Reader %s: head=%s done=%s".format(
        fanoutName, reader.head, reader.doneSet.toList.sorted.mkString("(", ", ", ")")
      ))
    }
    newJournal.close()
  }

  def main(args: Array[String]) {
    parseArgs(args.toList)
    if ((oldFolder eq null) || (newFolder eq null)) {
      usage()
      System.exit(0)
    }
    if (!oldFolder.exists || !oldFolder.isDirectory() || !oldFolder.canRead()) {
      println("The old folder (%s) must be readable.".format(oldFolder))
      System.exit(1)
    }
    if (!newFolder.exists || !newFolder.isDirectory() || !newFolder.canWrite()) {
      newFolder.mkdirs()
      if (!newFolder.exists || !newFolder.isDirectory() || !newFolder.canWrite()) {
        println("The new folder (%s) must be writable.".format(newFolder))
        System.exit(1)
      }
    }
    if (oldFolder.getCanonicalPath() == newFolder.getCanonicalPath()) {
      println("The old folder (%s) and new folder (%s) have the same canonical path.".format(
        oldFolder, newFolder))
      System.exit(1)
    }
    newFolder.listFiles().foreach { _.delete() }

    val rawQueues = oldjournal.Journal.getQueueNamesFromFolder(oldFolder)
    val queues = rawQueues.filterNot { _ contains '+' }.toList.sorted
    val fanoutQueues = new mutable.HashMap[String, List[String]]
    (rawQueues -- queues).foreach { name =>
      val segments = name.split("\\+", 2)
      val oldList = fanoutQueues.getOrElse(segments(0), Nil)
      fanoutQueues(segments(0)) = segments(1) :: oldList
    }

    queues.foreach { q => convertQueue(q, fanoutQueues.getOrElse(q, Nil)) }
  }
}
