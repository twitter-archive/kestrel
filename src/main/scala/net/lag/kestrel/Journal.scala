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

import net.lag.logging.Logger
import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel
import net.lag.configgy.{Config, ConfigMap}


// returned from journal replay
abstract case class JournalItem()
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
class Journal(queuePath: String, config: ConfigMap) {
  // whether to sync the journal file after each write
  val syncJournal = new OverlaySetting(Journal.syncJournal)

  private val log = Logger.get

  private val queueFile = new File(queuePath)

  private var writer: FileChannel = null
  private var reader: Option[FileChannel] = None
  private var replayer: Option[FileChannel] = None

  var size: Long = 0

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

  config.subscribe { c => configure(c.getOrElse(new Config)) }
  configure(config)

  def configure(config: ConfigMap) = {
    syncJournal set config.getBool("sync_journal")
    log.info("Configuring journal %s: sync_journal=%s", queuePath, syncJournal())
  }

  private def open(file: File): Unit = {
    writer = new FileOutputStream(file, true).getChannel
  }

  def open(): Unit = {
    open(queueFile)
  }

  def roll(xid: Int, openItems: List[QItem], queue: Iterable[QItem]): Unit = {
    writer.close
    val tmpFile = new File(queuePath + "." + Time.now)
    open(tmpFile)
    size = 0
    for (item <- openItems) {
      addWithXid(item)
      removeTentative()
    }
    saveXid(xid)
    for (item <- queue) {
      add(item)
    }
    writer.close
    tmpFile.renameTo(queueFile)
    open
  }

  def close(): Unit = {
    writer.close
    for (r <- reader) r.close
    reader = None
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

  def add(item: QItem) = {
    val blob = ByteBuffer.wrap(pack(item))
    size += write(false, CMD_ADDX.toByte, blob.limit)
    do {
      writer.write(blob)
    } while (blob.position < blob.limit)
    size += blob.limit
  }

  // used only to list pending transactions when recreating the journal.
  private def addWithXid(item: QItem) = {
    val blob = ByteBuffer.wrap(pack(item))

    // this method is only called from roll(), so the journal does not
    // need to be synced after a write.
    size += write(false, CMD_ADD_XID.toByte, item.xid, blob.limit)
    do {
      writer.write(blob)
    } while (blob.position < blob.limit)
    size += blob.limit
  }

  def remove() = {
    size += write(true, CMD_REMOVE.toByte)
  }

  def removeTentative() = {
    size += write(true, CMD_REMOVE_TENTATIVE.toByte)
  }

  private def saveXid(xid: Int) = {
    // this method is only called from roll(), so the journal does not
    // need to be synced after a write.
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
    val rj = new FileInputStream(queueFile).getChannel
    rj.position(pos)
    reader = Some(rj)
  }

  def fillReadBehind(f: QItem => Unit): Unit = {
    val pos = if (replayer.isDefined) replayer.get.position else writer.position
    for (rj <- reader) {
      if (rj.position == pos) {
        // we've caught up.
        rj.close
        reader = None
      } else {
        readJournalEntry(rj, false) match {
          case JournalItem.Add(item) => f(item)
          case _ =>
        }
      }
    }
  }

  def replay(name: String)(f: JournalItem => Unit): Unit = {
    size = 0
    try {
      val in = new FileInputStream(queueFile).getChannel
      replayer = Some(in)
      var done = false
      do {
        readJournalEntry(in, true) match {
          case JournalItem.EndOfFile => done = true
          case x: JournalItem => f(x)
        }
      } while (!done)
    } catch {
      case e: FileNotFoundException =>
        log.info("No transaction journal for '%s'; starting with empty queue.", name)
      case e: IOException =>
        log.error(e, "Exception replaying journal for '%s'", name)
        log.error("DATA MAY HAVE BEEN LOST!")
        // this can happen if the server hardware died abruptly in the middle
        // of writing a journal. not awesome but we should recover.
    }
    replayer = None
  }

  private def readJournalEntry(in: FileChannel, replaying: Boolean): JournalItem = {
    byteBuffer.rewind
    byteBuffer.limit(1)
    var x: Int = 0
    do {
      x = in.read(byteBuffer)
    } while (byteBuffer.position < byteBuffer.limit && x >= 0)

    if (x < 0) {
      JournalItem.EndOfFile
    } else {
      buffer(0) match {
        case CMD_ADD =>
          val data = readBlock(in)
          if (replaying) size += 5 + data.length
          JournalItem.Add(unpackOldAdd(data))
        case CMD_REMOVE =>
          if (replaying) size += 1
          JournalItem.Remove
        case CMD_ADDX =>
          val data = readBlock(in)
          if (replaying) size += 5 + data.length
          JournalItem.Add(unpack(data))
        case CMD_REMOVE_TENTATIVE =>
          if (replaying) size += 1
          JournalItem.RemoveTentative
        case CMD_SAVE_XID =>
          val xid = readInt(in)
          if (replaying) size += 5
          JournalItem.SavedXid(xid)
        case CMD_UNREMOVE =>
          val xid = readInt(in)
          if (replaying) size += 5
          JournalItem.Unremove(xid)
        case CMD_CONFIRM_REMOVE =>
          val xid = readInt(in)
          if (replaying) size += 5
          JournalItem.ConfirmRemove(xid)
        case CMD_ADD_XID =>
          val xid = readInt(in)
          val data = readBlock(in)
          val item = unpack(data)
          item.xid = xid
          if (replaying) size += 9 + data.length
          JournalItem.Add(item)
        case n =>
          throw new IOException("invalid opcode in journal: " + n.toInt)
      }
    }
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
    byteBuffer.limit
  }

  private def pack(item: QItem): Array[Byte] = {
    val bytes = new Array[Byte](item.data.length + 16)
    val buffer = ByteBuffer.wrap(bytes)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.putLong(item.addTime)
    buffer.putLong(item.expiry)
    buffer.put(item.data)
    bytes
  }

  private def unpack(data: Array[Byte]): QItem = {
    val buffer = ByteBuffer.wrap(data)
    val bytes = new Array[Byte](data.length - 16)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val addTime = buffer.getLong
    val expiry = buffer.getLong
    buffer.get(bytes)
    return QItem(addTime, expiry, bytes, 0)
  }

  private def unpackOldAdd(data: Array[Byte]): QItem = {
    val buffer = ByteBuffer.wrap(data)
    val bytes = new Array[Byte](data.length - 4)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val expiry = buffer.getInt
    buffer.get(bytes)
    return QItem(Time.now, if (expiry == 0) 0 else expiry * 1000, bytes, 0)
  }
}

object Journal {
  @volatile var syncJournal: Boolean = false
}
