package com.twitter.scarling

import java.io.{ByteArrayOutputStream, DataInputStream, DataOutputStream,
    File, FileInputStream, FileNotFoundException, FileOutputStream,
    IOException}
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.Queue

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


class PersistentQueue(private val persistencePath: String, val name: String) {
    private val log = Logger.get

    /* when a journal reaches maxJournalSize, the queue will wait until
     * it is empty, and will then rotate the journal.
     */
    var maxJournalSize = 16 * 1024 * 1024

    private val CMD_ADD = 0
    private val CMD_REMOVE = 1

    private val queuePath: String = new File(persistencePath, name).getCanonicalPath()

    // current size of all data in the queue:
    private var queueSize: Int = 0

    // # of items EVER added to the queue:
    private var _totalItems: Int = 0

    // # of items that were expired by the time they were read:
    private var _totalExpired: Int = 0

    // age (in milliseconds) of the last item read from the queue:
    private var _currentAge: Long = 0

    private var queue = new Queue[(Long, Array[Byte])]
    private var journal: FileOutputStream = null
    private var _journalSize: Int = 0

    // small temporary buffer for formatting ADD transactions into the journal:
    private var byteBuffer = new ByteArrayOutputStream(16)
    private var buffer = new DataOutputStream(byteBuffer)

    // sad way to write little-endian ints when journaling queue adds
    private val intWriter = new IntWriter(ByteOrder.LITTLE_ENDIAN)

    // force get/set operations to block while we're replaying any existing journal
    private val initialized = new Event
    private var closed = false


    def size = synchronized { queue.length }

    def totalItems = synchronized { _totalItems }

    def bytes = synchronized { queueSize }

    def journalSize = synchronized { _journalSize }

    def totalExpired = synchronized { _totalExpired }

    def currentAge = synchronized { _currentAge }

    private def pack(expiry: Int, data: Array[Byte]): Array[Byte] = {
        val bytes = new Array[Byte](data.length + 4)
        val buffer = ByteBuffer.wrap(bytes)
        buffer.order(ByteOrder.LITTLE_ENDIAN)
        buffer.putInt(expiry)
        buffer.put(data)
        bytes
    }

    private def unpack(data: Array[Byte]): (Int, Array[Byte]) = {
        val buffer = ByteBuffer.wrap(data)
        val bytes = new Array[Byte](data.length - 4)
        buffer.order(ByteOrder.LITTLE_ENDIAN)
        val expiry = buffer.getInt
        buffer.get(bytes)
        return (expiry, bytes)
    }

    /**
     * Add a value to the end of the queue, transactionally.
     */
    def add(value: Array[Byte], expiry: Int): Boolean = {
        initialized.waitFor
        synchronized {
            if (closed) {
                return false
            }

            val blob = pack(expiry, value)

            byteBuffer.reset()
            buffer.write(CMD_ADD)
            intWriter.writeInt(buffer, blob.length)
            byteBuffer.writeTo(journal)
            journal.write(blob)
            /* in theory, you might want to sync the file after each
             * transaction. however, the original starling doesn't.
             * i think if you can cope with a truncated journal file,
             * this is fine, because a non-synced file only matters on
             * catastrophic disk/machine failure.
             */
            // FIXME: deal with truncated journal files. :)
            //journal.getFD.sync
            _journalSize += (5 + blob.length)

            _totalItems += 1
            queue += (System.currentTimeMillis, blob)
            queueSize += blob.length
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
            val nowSecs = (now / 1000).toInt
            val (addTime, item) = queue.dequeue
            queueSize -= item.length
            checkRoll
            val (expiry, data) = unpack(item)

            if ((expiry == 0) || (expiry >= nowSecs)) {
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


    private def openJournal: Unit = {
        journal = new FileOutputStream(queuePath, true)
    }

    private def checkRoll: Unit = {
        if ((queue.length > 0) || (_journalSize < maxJournalSize)) {
            return
        }

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
            var offset = 0
            val intReader = new IntReader(ByteOrder.LITTLE_ENDIAN)

            log.info("Replaying transaction journal for '%s'", name)
            var eof = false
            while (!eof) {
                in.read() match {
                    case -1 => {
                        eof = true
                    }
                    case CMD_ADD => {
                        val size = intReader.readInt(in)
                        val data = new Array[Byte](size)
                        in.readFully(data)
                        queue += (System.currentTimeMillis, data)
                        queueSize += data.length
                        offset += (5 + data.length)
                    }
                    case CMD_REMOVE => {
                        queueSize -= queue.dequeue._2.length
                        offset += 1
                    }
                    case n => {
                        log.error("INVALID opcode in journal at byte %d: %d", offset, n)
                        throw new IOException("invalid opcode")
                    }
                }
            }

            _journalSize = offset
            log.info("Finished transaction journal for '%s' (%d items, %d bytes)", name, size, offset)
        } catch {
            case e: FileNotFoundException => {
                log.info("No transaction journal for '%s'; starting with empty queue.", name)
            }
            case e: IOException => {
                log.error(e, "Exception replaying journal for '%s'", name)
                log.error("DATA MAY HAVE BEEN LOST!")
                // FIXME: would it be better to just stop the server here?
            }
        }

        openJournal
    }
}
