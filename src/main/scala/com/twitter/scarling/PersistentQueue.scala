package com.twitter.scarling

import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.Queue
import net.lag.configgy.{Config, Configgy, ConfigMap}
import net.lag.logging.Logger


// why does java make this so hard? :/
// this is not threadsafe so may only be used as a local var.
class IntReader(private val order: ByteOrder) {
  val buffer = new Array[Byte](4)
  val byteBuffer = ByteBuffer.wrap(buffer)
  byteBuffer.order(order)

  def readInt(in: DataInputStream) = {
    in.readFully(buffer)
    byteBuffer.rewind
    byteBuffer.getInt
  }
}


// again, must be either behind a lock, or in a local var
class IntWriter(private val order: ByteOrder) {
  val buffer = new Array[Byte](4)
  val byteBuffer = ByteBuffer.wrap(buffer)
  byteBuffer.order(order)

  def writeInt(out: DataOutputStream, n: Int) = {
    byteBuffer.rewind
    byteBuffer.putInt(n)
    out.write(buffer)
  }
}


class PersistentQueue(private val persistencePath: String, val name: String,
                      val config: ConfigMap) {
  private val log = Logger.get

  private val CMD_ADD = 0
  private val CMD_REMOVE = 1
  private val CMD_ADDX = 2

  private val queuePath: String = new File(persistencePath, name).getCanonicalPath()

  // current size of all data in the queue:
  private var queueSize: Long = 0

  // # of items EVER added to the queue:
  private var _totalItems: Long = 0

  // # of items that were expired by the time they were read:
  private var _totalExpired: Long = 0

  // age (in milliseconds) of the last item read from the queue:
  private var _currentAge: Long = 0

  // # of items in the queue (including those not in memory)
  private var queueLength: Long = 0

  private var queue = new Queue[Array[Byte]]
  private var journal: FileOutputStream = null
  private var _journalSize: Long = 0
  private var readJournal: Option[FileInputStream] = None

  // small temporary buffer for formatting ADD transactions into the journal:
  private var byteBuffer = new ByteArrayOutputStream(16)
  private var buffer = new DataOutputStream(byteBuffer)

  // sad way to write little-endian ints when journaling queue adds
  private val intWriter = new IntWriter(ByteOrder.LITTLE_ENDIAN)

  // force get/set operations to block while we're replaying any existing journal
  private val initialized = new Event
  private var closed = false

  // attempting to add an item after the queue reaches this size will fail.
  var maxItems = Math.MAX_INT

  // maximum expiration time for this queue (seconds).
  var maxAge = 0

  config.subscribe(configure _)
  configure(Some(config))

  def length: Long = synchronized { queueLength }

  def totalItems: Long = synchronized { _totalItems }

  def bytes: Long = synchronized { queueSize }

  def journalSize: Long = synchronized { _journalSize }

  def totalExpired: Long = synchronized { _totalExpired }

  def currentAge: Long = synchronized { _currentAge }

  def configure(c: Option[ConfigMap]) = synchronized {
    for (config <- c) {
      maxItems = config("max_items", Math.MAX_INT)
      maxAge = config("max_age", 0)
    }
  }

  private def pack(addTime: Long, expiry: Long, data: Array[Byte]): Array[Byte] = {
    val bytes = new Array[Byte](data.length + 16)
    val buffer = ByteBuffer.wrap(bytes)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.putLong(addTime)
    buffer.putLong(expiry)
    buffer.put(data)
    bytes
  }

  private def unpack(data: Array[Byte]): (Long, Long, Array[Byte]) = {
    val buffer = ByteBuffer.wrap(data)
    val bytes = new Array[Byte](data.length - 16)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val addTime = buffer.getLong
    val expiry = buffer.getLong
    buffer.get(bytes)
    return (addTime, expiry, bytes)
  }

  private def unpackOldAdd(data: Array[Byte]): (Int, Array[Byte]) = {
    val buffer = ByteBuffer.wrap(data)
    val bytes = new Array[Byte](data.length - 4)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val expiry = buffer.getInt
    buffer.get(bytes)
    return (expiry, bytes)
  }

  private final def adjustExpiry(expiry: Long): Long = {
    if (maxAge > 0) {
      if (expiry > 0) (expiry min maxAge) else maxAge
    } else {
      expiry
    }
  }

  /**
   * Add a value to the end of the queue, transactionally.
   */
  def add(value: Array[Byte], expiry: Long): Boolean = {
    initialized.waitFor
    synchronized {
      if (closed || queueLength >= maxItems) {
        return false
      }

      if (!readJournal.isDefined && queueSize >= PersistentQueue.maxMemorySize) {
        startReadBehind
      }

      val addTime = System.currentTimeMillis
      val blob = pack(addTime, adjustExpiry(expiry), value)

      byteBuffer.reset()
      buffer.write(CMD_ADDX)
      intWriter.writeInt(buffer, blob.length)
      byteBuffer.writeTo(journal)
      journal.write(blob)
      /* in theory, you might want to sync the file after each
       * transaction. however, the original starling doesn't.
       * i think if you can cope with a truncated journal file,
       * this is fine, because a non-synced file only matters on
       * catastrophic disk/machine failure.
       */
      //journal.getFD.sync
      _journalSize += (5 + blob.length)

      _totalItems += 1
      queue += blob
      queueLength += 1
      queueSize += value.length
      true
    }
  }

  def add(value: Array[Byte]): Boolean = add(value, 0)

  /**
   * Remove an item from the queue, transactionally. If no item is
   * available, an empty byte array is returned.
   */
  def remove: Option[Array[Byte]] = {
    initialized.waitFor
    synchronized {
      if (queue.isEmpty || closed) {
        return None
      }

      journal.write(CMD_REMOVE)
      journal.getFD.sync
      _journalSize += 1

      val now = System.currentTimeMillis
      val (addTime, expiry, data) = unpack(queue.dequeue)
      queueLength -= 1
      queueSize -= data.length

      if ((queueLength == 0) && (_journalSize >= PersistentQueue.maxJournalSize)) {
        rollJournal
      }

      val realExpiry = adjustExpiry(expiry)
      if ((realExpiry == 0) || (realExpiry >= now)) {
        _currentAge = now - addTime
        Some(data)
      } else {
        _totalExpired += 1
        remove
      }
    }
  }

  /**
   * Close the queue's journal file. Not safe to call on an active queue.
   */
  def close = synchronized {
    closed = true
    journal.close()
  }

  def setup: Unit = synchronized {
    replayJournal
    initialized.set
  }


  private def startReadBehind: Unit = {

  }

  private def openJournal: Unit = {
    journal = new FileOutputStream(queuePath, true)
  }

  private def rollJournal: Unit = {
    log.info("Rolling journal file for '%s'", name)
    journal.close

    val backupFile = new File(queuePath + "." + System.currentTimeMillis)
    new File(queuePath).renameTo(backupFile)
    openJournal
    _journalSize = 0
    backupFile.delete
  }

  private def replayJournal: Unit = {
    queueSize = 0

    try {
      val fileIn = new FileInputStream(queuePath)
      val in = new DataInputStream(fileIn)
      var offset: Long = 0
      val intReader = new IntReader(ByteOrder.LITTLE_ENDIAN)

      log.info("Replaying transaction journal for '%s'", name)
      var eof = false
      while (!eof) {
        in.read() match {
          case -1 =>
            eof = true
          case CMD_ADD =>
            val size = intReader.readInt(in)
            val data = new Array[Byte](size)
            in.readFully(data)
            val (expiry, item) = unpackOldAdd(data)
            val blob = pack(System.currentTimeMillis, expiry.toLong * 1000, item)
            queue += blob
            queueSize += item.length
            queueLength += 1
            offset += (5 + data.length)
          case CMD_REMOVE =>
            queueSize -= queue.dequeue.length
            queueLength -= 1
            offset += 1
          case CMD_ADDX =>
            val size = intReader.readInt(in)
            val data = new Array[Byte](size)
            in.readFully(data)
            queue += data
            queueSize += data.length - 16
            queueLength += 1
            offset += (5 + data.length)
          case n =>
            log.error("INVALID opcode in journal at byte %d: %d", offset, n)
            throw new IOException("invalid opcode")
        }
      }

      _journalSize = offset
      log.info("Finished transaction journal for '%s' (%d items, %d bytes)", name, queueLength, offset)
    } catch {
      case e: FileNotFoundException =>
        log.info("No transaction journal for '%s'; starting with empty queue.", name)
      case e: IOException =>
        log.error(e, "Exception replaying journal for '%s'", name)
        log.error("DATA MAY HAVE BEEN LOST!")
        // this can happen if the server hardware died abruptly in the middle
        // of writing a journal. not awesome but we should recover.
    }

    openJournal
  }
}

object PersistentQueue {
  @volatile var maxJournalSize: Long = 16 * 1024 * 1024
  @volatile var maxMemorySize: Long = 128 * 1024 * 1024
}
