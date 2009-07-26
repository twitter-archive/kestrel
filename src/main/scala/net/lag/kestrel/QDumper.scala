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
import java.nio.channels.FileChannel
import scala.collection.mutable

class QueueDumper(filename: String) {
  var items = 0L
  var size = 0L
  var offset = 0L
  var xid = 0

  val queue = new mutable.Queue[Int] {
    def unget(item: Int) = prependElem(item)
  }
  val openTransactions = new mutable.HashMap[Int, Int]

  def apply() {
    val journal = new Journal(filename, false)

    try {
      val in = new FileInputStream(filename).getChannel
      var done = false
      do {
        journal.readJournalEntry(in) match {
          case (JournalItem.EndOfFile, _) =>
            done = true
          case (x, itemsize) =>
            dumpItem(x)
            offset += itemsize
        }
      } while (!done)

      println()
      val totalItems = queue.size + openTransactions.size
      val totalBytes = queue.foldLeft(0L) { _ + _ } + openTransactions.values.foldLeft(0L) { _ + _ }
      println("Journal size: %d bytes.".format(offset))
      println("%d items totalling %d bytes.".format(totalItems, totalBytes))
    } catch {
      case e: FileNotFoundException =>
        println("Can't open journal file: " + filename)
      case e: IOException =>
        println("Exception reading journal file: " + filename)
        e.printStackTrace()
    }
  }

  def dumpItem(item: JournalItem) {
    val now = Time.now
    if (!QDumper.quiet) print("%08x  ".format(offset & 0xffffffffL))
    item match {
      case JournalItem.Add(qitem) =>
        if (!QDumper.quiet) {
          print("ADD %-6d".format(qitem.data.size))
          if (qitem.xid > 0) {
            print(" xid=%d".format(qitem.xid))
          }
          if (qitem.expiry > 0) {
            if (qitem.expiry - now < 0) {
              print(" expired")
            } else {
              print(" exp=%d".format(qitem.expiry - now))
            }
          }
          println()
        }
        queue += qitem.data.size
      case JournalItem.Remove =>
        if (!QDumper.quiet) println("REM")
        queue.dequeue
      case JournalItem.RemoveTentative =>
        do {
          xid += 1
        } while (openTransactions contains xid)
        openTransactions(xid) = queue.dequeue
        if (!QDumper.quiet) println("RSV %d".format(xid))
      case JournalItem.SavedXid(sxid) =>
        if (!QDumper.quiet) println("XID %d".format(sxid))
        xid = sxid
      case JournalItem.Unremove(sxid) =>
        queue.unget(openTransactions.removeKey(sxid).get)
        if (!QDumper.quiet) println("CAN %d".format(sxid))
      case JournalItem.ConfirmRemove(sxid) =>
        if (!QDumper.quiet) println("ACK %d".format(sxid))
        openTransactions.removeKey(sxid)
      case x =>
        if (!QDumper.quiet) println(x)
    }
  }
}


object QDumper {
  val filenames = new mutable.ListBuffer[String]
  var quiet = false

  def usage() {
    println()
    println("usage: qdump.sh <journal-files...>")
    println("    describe the contents of a kestrel journal file")
    println()
    println("options:")
    println("    -q      quiet: don't describe every line, just the summary")
    println()
  }

  def parseArgs(args: List[String]): Unit = args match {
    case Nil =>
    case "--help" :: xs =>
      usage()
      System.exit(0)
    case "-q" :: xs =>
      quiet = true
      parseArgs(xs)
    case x :: xs =>
      filenames += x
      parseArgs(xs)
  }

  def main(args: Array[String]) {
    parseArgs(args.toList)
    if (filenames.size == 0) {
      usage()
      System.exit(0)
    }

    for (filename <- filenames) {
      println("Queue file: " + filename)
      new QueueDumper(filename)()
    }
  }
}
