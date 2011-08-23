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
package tools

import java.io.{FileNotFoundException, IOException}
import scala.collection.mutable
import com.twitter.conversions.time._
import com.twitter.util.{Duration, Time}

class QueueDumper(filename: String, quiet: Boolean, dump: Boolean, dumpRaw: Boolean) {
  var offset = 0L
  var operations = 0L
  var currentXid = 0

  val queue = new mutable.Queue[Int]
  val openTransactions = new mutable.HashMap[Int, Int]

  def verbose(s: String, args: Any*) {
    if (!quiet) {
      print(s.format(args: _*))
    }
  }

  def apply() {
    val journal = new Journal(filename, Duration.MaxValue)
    var lastDisplay = 0L

    try {
      for ((item, itemsize) <- journal.walk()) {
        operations += 1
        dumpItem(item)
        offset += itemsize
        if (quiet && !dumpRaw && offset - lastDisplay > 1024 * 1024) {
          print("\rReading journal: %-6s".format(Util.bytesToHuman(offset, 0)))
          Console.flush()
          lastDisplay = offset
        }
      }
      if (!dumpRaw) {
        print("\r" + (" " * 30))
      }

      if (!dumpRaw) {
        println()
        val totalItems = queue.size + openTransactions.size
        val totalBytes = queue.foldLeft(0L) { _ + _ } + openTransactions.values.foldLeft(0L) { _ + _ }
        println("Journal size: %d bytes, with %d operations.".format(offset, operations))
        println("%d items totalling %d bytes.".format(totalItems, totalBytes))
      }
    } catch {
      case e: FileNotFoundException =>
        Console.err.println("Can't open journal file: " + filename)
      case e: IOException =>
        Console.err.println("Exception reading journal file: " + filename)
        e.printStackTrace(Console.err)
    }
  }

  def dumpItem(item: JournalItem) {
    val now = Time.now
    verbose("%08x  ", offset & 0xffffffffL)
    item match {
      case JournalItem.Add(qitem) =>
        if (!quiet) {
          verbose("ADD %-6d", qitem.data.size)
          if (qitem.xid > 0) {
            verbose(" xid=%d", qitem.xid)
          }
          if (qitem.expiry.isDefined) {
            if (qitem.expiry.get - now < 0.milliseconds) {
              verbose(" expired")
            } else {
              verbose(" exp=%s", qitem.expiry.get - now)
            }
          }
          verbose("\n")
        }
        if (dump) {
          println("    " + new String(qitem.data, "ISO-8859-1"))
        } else if (dumpRaw) {
          print(new String(qitem.data, "ISO-8859-1"))
        }
        queue += qitem.data.size
      case JournalItem.Remove =>
        verbose("REM\n")
        queue.dequeue()
      case JournalItem.RemoveTentative(xid) =>
        val xxid = if (xid == 0) {
          do {
            currentXid += 1
          } while ((openTransactions contains currentXid) || (currentXid == 0))
          currentXid
        } else {
          xid
        }
        openTransactions(xxid) = queue.dequeue()
        verbose("RSV %d\n", xxid)
      case JournalItem.SavedXid(xid) =>
        verbose("XID %d\n", xid)
        currentXid = xid
      case JournalItem.Unremove(xid) =>
        openTransactions.remove(xid).get +=: queue
        verbose("CAN %d\n", xid)
      case JournalItem.ConfirmRemove(xid) =>
        verbose("ACK %d\n", xid)
        openTransactions.remove(xid)
      case JournalItem.Continue(qitem, xid) =>
        if (!quiet) {
          verbose("CON %-6d", qitem.data.size)
          if (qitem.xid > 0) {
            verbose(" xid=%d", qitem.xid)
          }
          if (qitem.expiry.isDefined) {
            if (qitem.expiry.get - now < 0.milliseconds) {
              verbose(" expired")
            } else {
              verbose(" exp=%s", qitem.expiry.get - now)
            }
          }
          verbose("\n")
        }
        if (dump) {
          println("    " + new String(qitem.data, "ISO-8859-1"))
        } else if (dumpRaw) {
          print(new String(qitem.data, "ISO-8859-1"))
        }
        openTransactions.remove(xid)
        queue += qitem.data.size
      case x =>
        verbose(x.toString)
    }
  }
}


object QDumper {
  val filenames = new mutable.ListBuffer[String]
  var quiet = false
  var dump = false
  var dumpRaw = false

  def usage() {
    println()
    println("usage: qdump.sh <journal-files...>")
    println("    describe the contents of a kestrel journal file")
    println()
    println("options:")
    println("    -q      quiet: don't describe every line, just the summary")
    println("    -d      dump contents of added items")
    println("    -A      dump only the raw contents of added items")
    println()
  }

  def parseArgs(args: List[String]) {
    args match {
      case Nil =>
      case "--help" :: xs =>
        usage()
        System.exit(0)
      case "-q" :: xs =>
        quiet = true
        parseArgs(xs)
      case "-d" :: xs =>
        dump = true
        parseArgs(xs)
      case "-A" :: xs =>
        dumpRaw = true
        quiet = true
        parseArgs(xs)
      case x :: xs =>
        filenames += x
        parseArgs(xs)
    }
  }

  def main(args: Array[String]) {
    parseArgs(args.toList)
    if (filenames.size == 0) {
      usage()
      System.exit(0)
    }

    for (filename <- filenames) {
      if (!quiet) println("Queue: " + filename)
      new QueueDumper(filename, quiet, dump, dumpRaw)()
    }
  }
}
