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
package oldjournal

import com.twitter.conversions.storage._
import com.twitter.util.Duration
import java.io.{FileNotFoundException, IOException}
import net.lag.kestrel.oldjournal._
import scala.collection.mutable


object QPacker {
  val filenames = new mutable.ListBuffer[String]
  var newFilename: String = null
  var quiet = false

  def usage() {
    println()
    println("usage: qpack.sh <journal-files...>")
    println("    pack one or more old (2.x) kestrel journal file(s) into a single new one")
    println()
    println("options:")
    println("    -q              quiet mode")
    println("    -f filename     new packed journal file")
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
    case "-f" :: x :: xs =>
      newFilename = x
      parseArgs(xs)
    case x :: xs =>
      filenames += x
      parseArgs(xs)
  }

  def main(args: Array[String]) {
    parseArgs(args.toList)
    if ((filenames.size == 0) || (newFilename eq null)) {
      usage()
      System.exit(0)
    }

    if (!quiet) println("Packing journals...")
    val packer = new JournalPacker(filenames)
    val journalState = packer { (bytes1, bytes2) =>
      if (!quiet) {
        print("\rPacking: %-6s %-6s".format(bytes1.bytes.toHuman, bytes2.bytes.toHuman))
        Console.flush()
      }
    }

    if (!quiet) {
      println("\rWriting new journal..." + (" " * 40))
      Console.flush()
    }

    val out = new Journal(newFilename, Duration.MaxValue)
    out.open()
    out.dump(journalState.openTransactions, journalState.items)
    out.close()

    if (!quiet) {
      print("\r" + (" " * 40) + "\r")
      println("Done. New journal size: %d".format(out.size))
    }
  }
}
