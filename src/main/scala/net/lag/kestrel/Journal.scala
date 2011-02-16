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
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel
import java.util.concurrent.Semaphore
import com.twitter.admin.BackgroundProcess
import com.twitter.conversions.storage._
import com.twitter.logging.Logger
import com.twitter.util.Time

case class BrokenItemException(lastValidPosition: Long, cause: Throwable) extends IOException(cause)

// returned from journal replay
abstract class JournalItem()
object JournalItem {
  case class Add(item: QItem) extends JournalItem
  case object Remove extends JournalItem
  case object RemoveTentative extends JournalItem
  case class SavedXid(xid: Int) extends JournalItem
  case class Unremove(xid: Int) extends JournalItem
  case class ConfirmRemove(xid: Int) extends JournalItem
  case object EndOfFile extends JournalItem
}

/**
 * Codes for working with the journal file for a PersistentQueue.
 */
class Journal(queuePath: String, queueName: String, syncJournal: => Boolean, multifile: => Boolean) {
  private val log = Logger.get

  private val queueFile = new File(queuePath, queueName)

  private var writer: FileChannel = null
  private var reader: Option[FileChannel] = None
  private var readerFilename: Option[String] = None
  private var replayer: Option[FileChannel] = None
  private var replayerFilename: Option[String] = None

  var size: Long = 0
  @volatile var closed: Boolean = false

  // small temporary buffer for formatting operations into the journal:
  private val buffer = new Array[Byte](16)
  private val byteBuffer = ByteBuffer.wrap(buffer)
  byteBuffer.order(ByteOrder.LITTLE_ENDIAN)

  private val CMD_ADD = 0
  private val CMD_REMOVE = 1
  private val CMD_ADDX = 2
  private val CMD_REMOVE_TENTATIVE = 3
  private val CMD_SAVE_XID = 4
  private val CMD_UNREMOVE = 5
  private val CMD_CONFIRM_REMOVE = 6
  private val CMD_ADD_XID = 7


  def this(fullPath: String, syncJournal: => Boolean) =
    this(new File(fullPath).getParent(), new File(fullPath).getName(), syncJournal, false)

  private def open(file: File): Unit = {
    writer = new FileOutputStream(file, true).getChannel
  }

  def open(): Unit = {
    open(queueFile)
  }

  def totalSize: Long = {
    (queueFile :: Journal.orderedFilesForQueue(new File(queuePath), queueName).map {
      new File(queuePath, _)
    }).foldLeft(0L) { (sum, f) => sum + f.length() }
  }

  def roll(xid: Int, openItems: Seq[QItem], queue: Iterable[QItem]) {
    writer.close
    val tmpFile = new File(queuePath, queueName + "~~" + Time.now.inMilliseconds)
    open(tmpFile)
    dump(xid, openItems, queue)
    writer.close
    tmpFile.renameTo(queueFile)
    open
  }

  def dump(xid: Int, openItems: Seq[QItem], queue: Iterable[QItem]) {
    size = 0
    for (item <- openItems) {
      addWithXid(item)
      removeTentative(false)
    }
    saveXid(xid)
    for (item <- queue) {
      add(false, item)
    }
    if (syncJournal) writer.force(false)
  }

  def close(): Unit = {
    writer.close
    reader.foreach { _.close }
    reader = None
    readerFilename = None
    closed = true
    packerSemaphore.release()
    packer.join()
  }

  def erase(): Unit = {
    try {
      close()
      queueFile.delete
    } catch {
      case _ =>
    }
  }

  def inReadBehind(): Boolean = reader.isDefined

  def isReplaying(): Boolean = replayer.isDefined

  private def add(allowSync: Boolean, item: QItem): Unit = {
    val blob = item.pack(CMD_ADDX.toByte, false)
    do {
      writer.write(blob)
    } while (blob.position < blob.limit)
    if (allowSync && syncJournal) writer.force(false)
    size += blob.limit
  }

  def add(item: QItem): Unit = add(true, item)

  // used only to list pending transactions when recreating the journal.
  private def addWithXid(item: QItem) = {
    val blob = item.pack(CMD_ADD_XID.toByte, true)
    do {
      writer.write(blob)
    } while (blob.position < blob.limit)
    // only called from roll(), so the journal does not need to be synced after a write.
    size += blob.limit
  }

  def remove() = {
    size += write(true, CMD_REMOVE.toByte)
  }

  private def removeTentative(allowSync: Boolean): Unit = {
    size += write(allowSync, CMD_REMOVE_TENTATIVE.toByte)
  }

  def removeTentative(): Unit = removeTentative(true)

  private def saveXid(xid: Int) = {
    // only called from roll(), so the journal does not need to be synced after a write.
    size += write(false, CMD_SAVE_XID.toByte, xid)
  }

  def unremove(xid: Int) = {
    size += write(true, CMD_UNREMOVE.toByte, xid)
  }

  def confirmRemove(xid: Int) = {
    size += write(true, CMD_CONFIRM_REMOVE.toByte, xid)
  }

  def startReadBehind(): Unit = {
    val pos = if (replayer.isDefined) replayer.get.position else writer.position
    val filename = if (replayerFilename.isDefined) replayerFilename.get else queueName
    val rj = new FileInputStream(new File(queuePath, filename)).getChannel
    rj.position(pos)
    reader = Some(rj)
    readerFilename = Some(filename)
    log.debug("Read-behind on '%s' starting at file %s", queueName, readerFilename.get)
  }

  // not tail recursive, but should only recurse once.
  def fillReadBehind(f: QItem => Unit): Unit = {
    val pos = if (replayer.isDefined) replayer.get.position else writer.position
    val filename = if (replayerFilename.isDefined) replayerFilename.get else queueName

    reader.foreach { rj =>
      if (rj.position == pos && readerFilename.get == filename) {
        // we've caught up.
        rj.close
        reader = None
        readerFilename = None
      } else {
        readJournalEntry(rj) match {
          case (JournalItem.Add(item), _) =>
            f(item)
          case (JournalItem.EndOfFile, _) =>
            // move to next file and try again.
            val oldFilename = readerFilename.get
            readerFilename = Journal.journalAfter(new File(queuePath), queueName, readerFilename.get)
            reader = Some(new FileInputStream(new File(queuePath, readerFilename.get)).getChannel)
            log.debug("Read-behind on '%s' moving from file %s to %s", queueName, oldFilename, readerFilename.get)
            fillReadBehind(f)
            packerSemaphore.release()
          case (_, _) =>
        }
      }
    }
  }

  def replay(name: String)(f: JournalItem => Unit): Unit = {
    Journal.journalsForQueue(new File(queuePath), queueName).foreach { filename =>
      replayFile(name, filename)(f)
    }
  }

  def replayFile(name: String, filename: String)(f: JournalItem => Unit): Unit = {
    log.debug("Replaying '%s' file %s", name, filename)
    size = 0
    var lastUpdate = 0L
    try {
      val in = new FileInputStream(new File(queuePath, filename).getCanonicalPath).getChannel
      replayer = Some(in)
      replayerFilename = Some(filename)
      try {
        var done = false
        do {
          readJournalEntry(in) match {
            case (JournalItem.EndOfFile, _) => done = true
            case (x, itemsize) =>
              size += itemsize
              f(x)
              if (size > lastUpdate + 10.megabytes.inBytes) {
                log.info("Continuing to read '%s' journal (%s); %s so far...", name, filename, size.bytes.toHuman)
                lastUpdate = size
              }
          }
        } while (!done)
      } catch {
        case e: BrokenItemException =>
          log.error(e, "Exception replaying journal for '%s': %s", name, filename)
          log.error("DATA MAY HAVE BEEN LOST! Truncated entry will be deleted.")
          truncateJournal(e.lastValidPosition)
      }
    } catch {
      case e: FileNotFoundException =>
        log.info("No transaction journal for '%s'; starting with empty queue.", name)
      case e: IOException =>
        log.error(e, "Exception replaying journal for '%s': %s", name, filename)
        log.error("DATA MAY HAVE BEEN LOST!")
        // this can happen if the server hardware died abruptly in the middle
        // of writing a journal. not awesome but we should recover.
    }
    replayer = None
    replayerFilename = None
  }

  private def truncateJournal(position: Long) {
    val trancateWriter = new FileOutputStream(queueFile, true).getChannel
    try {
      trancateWriter.truncate(position)
    } finally {
      trancateWriter.close()
    }
  }

  def readJournalEntry(in: FileChannel): (JournalItem, Int) = {
    byteBuffer.rewind
    byteBuffer.limit(1)
    val lastPosition = in.position
    var x: Int = 0
    do {
      x = in.read(byteBuffer)
    } while (byteBuffer.position < byteBuffer.limit && x >= 0)

    if (x < 0) {
      (JournalItem.EndOfFile, 0)
    } else {
      try {
        buffer(0) match {
          case CMD_ADD =>
            val data = readBlock(in)
            (JournalItem.Add(QItem.unpackOldAdd(data)), 5 + data.length)
          case CMD_REMOVE =>
            (JournalItem.Remove, 1)
          case CMD_ADDX =>
            val data = readBlock(in)
            (JournalItem.Add(QItem.unpack(data)), 5 + data.length)
          case CMD_REMOVE_TENTATIVE =>
            (JournalItem.RemoveTentative, 1)
          case CMD_SAVE_XID =>
            val xid = readInt(in)
            (JournalItem.SavedXid(xid), 5)
          case CMD_UNREMOVE =>
            val xid = readInt(in)
            (JournalItem.Unremove(xid), 5)
          case CMD_CONFIRM_REMOVE =>
            val xid = readInt(in)
            (JournalItem.ConfirmRemove(xid), 5)
          case CMD_ADD_XID =>
            val xid = readInt(in)
            val data = readBlock(in)
            val item = QItem.unpack(data)
            item.xid = xid
            (JournalItem.Add(item), 9 + data.length)
          case n =>
            throw new BrokenItemException(lastPosition, new IOException("invalid opcode in journal: " + n.toInt + " at position " + (in.position - 1)))
        }
      } catch {
        case ex: IOException =>
          throw new BrokenItemException(lastPosition, ex)
      }
    }
  }

  def walk(): Iterator[(JournalItem, Int)] = {
    val in = new FileInputStream(new File(queuePath, queueName)).getChannel
    def next(): Stream[(JournalItem, Int)] = {
      readJournalEntry(in) match {
        case (JournalItem.EndOfFile, _) =>
          in.close()
          Stream.Empty
        case x =>
          new Stream.Cons(x, next())
      }
    }
    next().iterator
  }

  private def readBlock(in: FileChannel): Array[Byte] = {
    val size = readInt(in)
    val data = new Array[Byte](size)
    val dataBuffer = ByteBuffer.wrap(data)
    var x: Int = 0
    do {
      x = in.read(dataBuffer)
    } while (dataBuffer.position < dataBuffer.limit && x >= 0)
    if (x < 0) {
      // we never expect EOF when reading a block.
      throw new IOException("Unexpected EOF")
    }
    data
  }

  private def readInt(in: FileChannel): Int = {
    byteBuffer.rewind
    byteBuffer.limit(4)
    var x: Int = 0
    do {
      x = in.read(byteBuffer)
    } while (byteBuffer.position < byteBuffer.limit && x >= 0)
    if (x < 0) {
      // we never expect EOF when reading an int.
      throw new IOException("Unexpected EOF")
    }
    byteBuffer.rewind
    byteBuffer.getInt()
  }

  private def write(allowSync: Boolean, items: Any*): Int = {
    byteBuffer.clear
    for (item <- items) item match {
      case b: Byte => byteBuffer.put(b)
      case i: Int => byteBuffer.putInt(i)
    }
    byteBuffer.flip
    while (byteBuffer.position < byteBuffer.limit) {
      writer.write(byteBuffer)
    }
    if (allowSync && syncJournal) writer.force(false)
    byteBuffer.limit
  }

  def rotate() {
    writer.close
    var rotatedFile = queueName + "." + Time.now.inMilliseconds
    while (new File(queuePath, rotatedFile).exists) {
      Thread.sleep(1)
      rotatedFile = queueName + "." + Time.now.inMilliseconds
    }
    new File(queuePath, queueName).renameTo(new File(queuePath, rotatedFile))
    size = 0
    open

    if (readerFilename == Some(queueName)) {
      readerFilename = Some(rotatedFile)
    }
    packerSemaphore.release()
  }

  val packerSemaphore = new Semaphore(0)
  val packer = BackgroundProcess.spawnDaemon("pack:" + queueName) {
    while (!closed) {
      packerSemaphore.acquire()
      if (!closed) pack()
    }
  }

  private def pack() {
    val filenames = Journal.journalsBefore(new File(queuePath), queueName, readerFilename.getOrElse(queueName))
    if (filenames.size > 1) {
      log.info("Packing journals for '%s': %s", queueName, filenames.mkString(", "))
      val tmpFile = new File(queuePath, queueName + "~~" + Time.now.inMilliseconds)
      val packer = new JournalPacker(filenames.map { new File(queuePath, _).getAbsolutePath },
                                     tmpFile.getAbsolutePath)

      val journal = packer { (bytes1, bytes2) =>
        if (bytes1 == 0 && bytes2 == 0) {
          log.info("Packing '%s' into new journal.", queueName)
        } else {
          log.info("Packing '%s': %s so far (%s trailing)", queueName, bytes1.bytes.toHuman, bytes2.bytes.toHuman)
        }
      }

      log.info("Packing '%s' -- erasing old files.", queueName)
      val packFile = new File(queuePath, queueName + ".0.pack")
      tmpFile.renameTo(packFile)
      Journal.cleanUpFinishedPack(new File(queuePath), filenames, packFile.getAbsolutePath)
      log.info("Packing '%s' done: %s", queueName, Journal.journalsForQueue(new File(queuePath), queueName).mkString(", "))
    }
  }
}

object Journal {
  def getQueueNamesFromFolder(path: File): Seq[String] = {
    path.list().filter { name =>
      !(name contains "~~")
    }.map { name =>
      name.split('.')(0)
    }
  }

  def orderedFilesForQueue(path: File, queueName: String): List[String] = {
    val totalFiles = path.list()
    totalFiles.filter { _ startsWith (queueName + ".") }.sortBy { name =>
      name.split('.')(1).toLong
    }.toList
  }

  def cleanUpFinishedPack(path: File, oldFiles: Seq[String], packedFile: String) = {
    oldFiles.foreach { f => new File(path, f).delete() }
    val postPackedFile = packedFile.substring(0, packedFile.length - 5)
    new File(path, packedFile) renameTo new File(path, postPackedFile)
    postPackedFile
  }

  def journalsForQueue(path: File, queueName: String): List[String] = {
    val timedFiles = orderedFilesForQueue(path, queueName)
    val fixedTimedFiles: List[String] = if (timedFiles.exists { _ endsWith ".pack" }) {
      // incomplete packing job died after creating the combined file, before erasing the others.
      // finish the job.
      val doomed = timedFiles.takeWhile { f => !(f endsWith ".pack") }
      val packedFile = timedFiles.find { _ endsWith ".pack" }.get
      val postPackedFile = cleanUpFinishedPack(path, doomed, packedFile)
      List(postPackedFile) ++ (timedFiles.dropWhile { f => !(f endsWith ".pack") }.drop(1))
    } else {
      timedFiles
    }
    val currentFile = path.list().filter { _ == queueName }
    val rv = fixedTimedFiles ++ currentFile
    if (rv.size == 0) {
      List(queueName)
    } else {
      rv
    }
  }

  def journalsBefore(path: File, queueName: String, filename: String): Seq[String] = {
    journalsForQueue(path, queueName).takeWhile { _ != filename }
  }

  def journalAfter(path: File, queueName: String, filename: String): Option[String] = {
    journalsForQueue(path, queueName).dropWhile { _ != filename }.drop(1).headOption
  }
}
