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
import java.util.concurrent.{LinkedBlockingQueue, ScheduledExecutorService}
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.ostrich.admin.BackgroundProcess
import com.twitter.util.{Future, Duration, Time}

case class BrokenItemException(lastValidPosition: Long, cause: Throwable) extends IOException(cause)

case class Checkpoint(filename: String, reservedItems: Seq[QItem])
case class PackRequest(journal: Journal, checkpoint: Checkpoint, openItems: Iterable[QItem],
                       pentUpDeletes: Int, queueState: Iterable[QItem])

// returned from journal replay
abstract class JournalItem()
object JournalItem {
  case class Add(item: QItem) extends JournalItem
  case object Remove extends JournalItem
  case class RemoveTentative(xid: Int) extends JournalItem
  case class SavedXid(xid: Int) extends JournalItem
  case class Unremove(xid: Int) extends JournalItem
  case class ConfirmRemove(xid: Int) extends JournalItem
  case class Continue(item: QItem, xid: Int) extends JournalItem
  case object EndOfFile extends JournalItem
}

/**
 * Codes for working with the journal file for a PersistentQueue.
 */
class Journal(queuePath: File, queueName: String, syncScheduler: ScheduledExecutorService, syncJournal: Duration) {
  import Journal._

  private val log = Logger.get(getClass)

  private val queueFile = new File(queuePath, queueName)

  private var writer: PeriodicSyncFile = null
  private var reader: Option[FileChannel] = None
  private var readerFilename: Option[String] = None
  private var replayer: Option[FileChannel] = None
  private var replayerFilename: Option[String] = None

  // size of the current file, so far
  var size: Long = 0

  // size of the previous files combined
  @volatile var archivedSize: Long = 0

  @volatile var closed: Boolean = false

  @volatile var checkpoint: Option[Checkpoint] = None
  var removesSinceReadBehind = 0

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
  private val CMD_CONTINUE = 8
  private val CMD_REMOVE_TENTATIVE_XID = 9

  def this(fullPath: String, syncJournal: Duration) =
    this(new File(fullPath).getParentFile, new File(fullPath).getName, null, syncJournal)

  def this(fullPath: String) = this(fullPath, Duration.MaxValue)

  private def open(file: File) {
    writer = new PeriodicSyncFile(file, syncScheduler, syncJournal)
  }

  def open() {
    open(queueFile)
  }

  def calculateArchiveSize() {
    val files = Journal.archivedFilesForQueue(queuePath, queueName)
    archivedSize = files.foldLeft(0L) { (sum, filename) =>
      sum + new File(queuePath, filename).length()
    }
  }

  private def uniqueFile(infix: String, suffix: String = ""): File = {
    var file = new File(queuePath, queueName + infix + Time.now.inMilliseconds + suffix)
    while (!file.createNewFile()) {
      Thread.sleep(1)
      file = new File(queuePath, queueName + infix + Time.now.inMilliseconds + suffix)
    }
    file
  }

  def rotate(reservedItems: Seq[QItem], setCheckpoint: Boolean): Option[Checkpoint] = {
    writer.close()
    val rotatedFile = uniqueFile(".")
    new File(queuePath, queueName).renameTo(rotatedFile)
    size = 0
    calculateArchiveSize()
    open()

    if (readerFilename == Some(queueName)) {
      readerFilename = Some(rotatedFile.getName)
    }

    if (setCheckpoint && !checkpoint.isDefined) {
      checkpoint = Some(Checkpoint(rotatedFile.getName, reservedItems))
    }
    checkpoint
  }

  def rewrite(reservedItems: Seq[QItem], queue: Iterable[QItem]) {
    writer.close()
    val tempFile = uniqueFile("~~")
    open(tempFile)
    dump(reservedItems, queue)
    writer.close()

    val packFile = uniqueFile(".", ".pack")
    tempFile.renameTo(packFile)
    // cleanup the .pack file:
    val files = Journal.archivedFilesForQueue(queuePath, queueName)
    new File(queuePath, files(0)).renameTo(queueFile)
    calculateArchiveSize()
    open()
  }

  def dump(reservedItems: Iterable[QItem], openItems: Iterable[QItem], pentUpDeletes: Int, queue: Iterable[QItem]) {
    size = 0
    for (item <- reservedItems) {
      add(item)
      removeTentative(item.xid)
    }
    for (item <- openItems) {
      add(item)
    }
    val empty = Array[Byte]()
    for (i <- 0 until pentUpDeletes) {
      add(false, QItem(Time.now, None, empty, 0))
    }
    for (item <- queue) {
      add(false, item)
    }
  }

  def dump(reservedItems: Iterable[QItem], queue: Iterable[QItem]) {
    dump(reservedItems, Nil, 0, queue)
  }

  def startPack(checkpoint: Checkpoint, openItems: Iterable[QItem], queueState: Seq[QItem]) {
    val knownXids = checkpoint.reservedItems.map { _.xid }.toSet
    val currentXids = openItems.map { _.xid }.toSet
    val newlyOpenItems = openItems.filter { x => !(knownXids contains x.xid) }
    val newlyClosedItems = checkpoint.reservedItems.filter { x => !(currentXids contains x.xid) }
    val negs = removesSinceReadBehind - newlyClosedItems.size // newly closed are already accounted for.

    outstandingPackRequests.incrementAndGet()
    packerQueue.add(PackRequest(this, checkpoint, newlyOpenItems, negs, queueState))
  }

  def close() {
    writer.close()
    reader.foreach { _.close() }
    reader = None
    readerFilename = None
    closed = true
    waitForPacksToFinish()
  }

  def erase() {
    try {
      close()
      Journal.archivedFilesForQueue(queuePath, queueName).foreach { filename =>
        new File(queuePath, filename).delete()
      }
      queueFile.delete()
    } catch {
      case _ =>
    }
  }

  def inReadBehind: Boolean = reader.isDefined

  def isReplaying: Boolean = replayer.isDefined

  private def add(allowSync: Boolean, item: QItem): Future[Unit] = {
    val blob = item.pack(CMD_ADDX.toByte)
    size += blob.limit
    writer.write(blob)
  }

  def add(item: QItem): Future[Unit] = add(true, item)

  def continue(xid: Int, item: QItem): Future[Unit] = {
    removesSinceReadBehind += 1
    val blob = item.pack(CMD_CONTINUE.toByte, xid)
    size += blob.limit
    writer.write(blob)
  }

  def remove() {
    write(CMD_REMOVE.toByte)
    if (inReadBehind) removesSinceReadBehind += 1
  }

  def removeTentative(xid: Int) {
    write(CMD_REMOVE_TENTATIVE_XID.toByte, xid)
  }

  def unremove(xid: Int) {
    write(CMD_UNREMOVE.toByte, xid)
  }

  def confirmRemove(xid: Int) {
    write(CMD_CONFIRM_REMOVE.toByte, xid)
    if (inReadBehind) removesSinceReadBehind += 1
  }

  def startReadBehind() {
    val pos = if (replayer.isDefined) replayer.get.position else writer.position
    val filename = if (replayerFilename.isDefined) replayerFilename.get else queueName
    val rj = new FileInputStream(new File(queuePath, filename)).getChannel
    rj.position(pos)
    reader = Some(rj)
    readerFilename = Some(filename)
    removesSinceReadBehind = 0
    log.debug("Read-behind on '%s' starting at file %s", queueName, readerFilename.get)
  }

  // not tail recursive, but should only recurse once.
  def fillReadBehind(gotItem: QItem => Unit)(gotCheckpoint: Checkpoint => Unit): Unit = {
    val pos = if (replayer.isDefined) replayer.get.position else writer.position
    val filename = if (replayerFilename.isDefined) replayerFilename.get else queueName

    reader.foreach { rj =>
      if (rj.position == pos && readerFilename.get == filename) {
        // we've caught up.
        rj.close()
        reader = None
        readerFilename = None
      } else {
        readJournalEntry(rj) match {
          case (JournalItem.Add(item), _) =>
            gotItem(item)
          case (JournalItem.Remove, _) =>
            removesSinceReadBehind -= 1
          case (JournalItem.ConfirmRemove(_), _) =>
            removesSinceReadBehind -= 1
          case (JournalItem.Continue(item, xid), _) =>
            removesSinceReadBehind -= 1
            gotItem(item)
          case (JournalItem.EndOfFile, _) =>
            // move to next file and try again.
            val oldFilename = readerFilename.get
            rj.close()
            readerFilename = Journal.journalAfter(queuePath, queueName, readerFilename.get)
            reader = Some(new FileInputStream(new File(queuePath, readerFilename.get)).getChannel)
            log.info("Read-behind on '%s' moving from file %s to %s", queueName, oldFilename, readerFilename.get)
            if (checkpoint.isDefined && checkpoint.get.filename == oldFilename) {
              gotCheckpoint(checkpoint.get)
            }
            fillReadBehind(gotItem)(gotCheckpoint)
          case (_, _) =>
        }
      }
    }
  }

  def replay(f: JournalItem => Unit) {
    // first, erase any lingering temp files.
    queuePath.list().filter {
      _.startsWith(queueName + "~~")
    }.foreach { filename =>
      new File(queuePath, filename).delete()
    }
    Journal.journalsForQueue(queuePath, queueName).foreach { filename =>
      replayFile(queueName, filename)(f)
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
            case (JournalItem.EndOfFile, _) =>
              done = true
            case (x, itemsize) =>
              size += itemsize
              f(x)
              if (size > lastUpdate + 10.megabytes.inBytes) {
                log.info("Continuing to read '%s' journal (%s); %s so far...", name, filename, size.bytes.toHuman())
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
            (JournalItem.RemoveTentative(0), 1)
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
          case CMD_CONTINUE =>
            val xid = readInt(in)
            val data = readBlock(in)
            val item = QItem.unpack(data)
            (JournalItem.Continue(item, xid), 9 + data.length)
          case CMD_REMOVE_TENTATIVE_XID =>
            val xid = readInt(in)
            (JournalItem.RemoveTentative(xid), 5)
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
    byteBuffer.getInt
  }

  private def write(items: Any*): Future[Unit] = {
    byteBuffer.clear
    for (item <- items) item match {
      case b: Byte => byteBuffer.put(b)
      case i: Int => byteBuffer.putInt(i)
    }
    byteBuffer.flip
    val future = writer.write(byteBuffer)
    size += byteBuffer.limit
    future
  }

  val outstandingPackRequests = new AtomicInteger(0)

  def waitForPacksToFinish() {
    while (outstandingPackRequests.get() > 0) {
      Thread.sleep(10)
    }
  }

  private def pack(state: PackRequest) {
    val oldFilenames =
      Journal.journalsBefore(queuePath, queueName, state.checkpoint.filename) ++
      List(state.checkpoint.filename)
    log.info("Packing journals for '%s': %s", queueName, oldFilenames.mkString(", "))

    val tempFile = uniqueFile("~~")
    val newJournal = new Journal(tempFile.getAbsolutePath)
    newJournal.open()
    newJournal.dump(state.checkpoint.reservedItems, state.openItems, state.pentUpDeletes, state.queueState)
    newJournal.close()

    log.info("Packing '%s' -- erasing old files.", queueName)
    val packFile = new File(queuePath, state.checkpoint.filename + ".pack")
    tempFile.renameTo(packFile)
    calculateArchiveSize()
    log.info("Packing '%s' done: %s", queueName, Journal.journalsForQueue(queuePath, queueName).mkString(", "))

    checkpoint = None
  }
}

object Journal {
  def getQueueNamesFromFolder(path: File): Set[String] = {
    path.list().filter { name =>
      !(name contains "~~")
    }.map { name =>
      name.split('.')(0)
    }.toSet
  }

  /**
   * A .pack file is atomically moved into place only after it contains a summary of the contents
   * of every journal file with a lesser-or-equal timestamp. If we find such a file, it's safe and
   * race-free to erase the older files and move the .pack file into place.
   */
  private def cleanUpPackedFiles(path: File, files: List[(String, Long)]): Boolean = {
    val packFile = files.find { case (filename, timestamp) =>
      filename endsWith ".pack"
    }
    if (packFile.isDefined) {
      val (packFilename, packTimestamp) = packFile.get
      val doomed = files.filter { case (filename, timestamp) =>
        (timestamp <= packTimestamp) && !(filename endsWith ".pack")
      }
      doomed.foreach { case (filename, timestamp) =>
        new File(path, filename).delete()
      }
      val newFilename = packFilename.substring(0, packFilename.length - 5)
      new File(path, packFilename).renameTo(new File(path, newFilename))
      true
    } else {
      false
    }
  }

  /**
   * Find all the archived (non-current) journal files for a queue, sort them in replay order (by
   * timestamp), and erase the remains of any unfinished business that we find along the way.
   */
  @tailrec
  def archivedFilesForQueue(path: File, queueName: String): List[String] = {
    val totalFiles = path.list()
    if (totalFiles eq null) {
      // directory is gone.
      Nil
    } else {
      val timedFiles = totalFiles.filter {
        _.startsWith(queueName + ".")
      }.map { filename =>
        (filename, filename.split('.')(1).toLong)
      }.sortBy { case (filename, timestamp) =>
        timestamp
      }.toList

      if (cleanUpPackedFiles(path, timedFiles)) {
        // probably only recurses once ever.
        archivedFilesForQueue(path, queueName)
      } else {
        timedFiles.map { case (filename, timestamp) => filename }
      }
    }
  }

  def journalsForQueue(path: File, queueName: String): List[String] = {
    archivedFilesForQueue(path, queueName) ++ List(queueName)
  }

  def journalsBefore(path: File, queueName: String, filename: String): Seq[String] = {
    journalsForQueue(path, queueName).takeWhile { _ != filename }
  }

  def journalAfter(path: File, queueName: String, filename: String): Option[String] = {
    journalsForQueue(path, queueName).dropWhile { _ != filename }.drop(1).headOption
  }

  val packerQueue = new LinkedBlockingQueue[PackRequest]()
  val packer = BackgroundProcess.spawnDaemon("journal-packer") {
    while (true) {
      val request = packerQueue.take()
      try {
        request.journal.pack(request)
        request.journal.outstandingPackRequests.decrementAndGet()
      } catch {
        case e: Throwable =>
          Logger.get(getClass).error(e, "Uncaught exception in packer: %s", e)
      }
    }
  }
}
