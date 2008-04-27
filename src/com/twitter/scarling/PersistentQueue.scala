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
    
    private var queue = new Queue[Array[Byte]]
    private var journal: FileOutputStream = null
    private var _journalSize: Int = 0
    
    // small temporary buffer for formatting ADD transactions into the journal:
    private var byteBuffer = new ByteArrayOutputStream(16)
    private var buffer = new DataOutputStream(byteBuffer)
    
    replayJournal
    
    
    def size = synchronized { queue.length }

    def totalItems = synchronized { _totalItems }
    
    def bytes = synchronized { queueSize }
    
    def journalSize = synchronized { _journalSize }
    
    /**
     * Add a value to the end of the queue, transactionally.
     */
    def add(value: Array[Byte]): Unit = synchronized {
        byteBuffer.reset()
        buffer.write(CMD_ADD)
        buffer.writeInt(value.length)
        byteBuffer.writeTo(journal)
        journal.write(value)
        journal.getFD.sync
        _journalSize += (5 + value.length)
        
        _totalItems += 1
        queue += value
        queueSize += value.length
    }
    
    /**
     * Remove an item from the queue, transactionally. If no item is
     * available, an empty byte array is returned.
     */
    def remove: Option[Array[Byte]] = synchronized {
        if (queue.isEmpty) {
            return None
        }
        
        journal.write(CMD_REMOVE)
        journal.getFD.sync
        _journalSize += 1
        
        val item = queue.dequeue
        queueSize -= item.length
        checkRoll
        Some(item)
    }

    /**
     * Close the queue's journal file. Not safe to call on an active queue.
     */
    def close = {
        journal.close()
    }
    
    
    private def openJournal: Unit = {
        journal = new FileOutputStream(queuePath)
        _journalSize = 0
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
        backupFile.delete
    }
    
    private def replayJournal: Unit = {
        log.info("Replaying transaction journal for '%s'", name)
        queueSize = 0
        
        try {
            val fileIn = new FileInputStream(queuePath)
            val in = new DataInputStream(fileIn)
            var offset = 0
            val intReader = new IntReader(ByteOrder.LITTLE_ENDIAN)
            
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
                        queue += data
                        queueSize += data.length
                        offset += (5 + data.length)
                        _totalItems += 1
                    }
                    case CMD_REMOVE => {
                        queueSize -= queue.dequeue.length
                        offset += 1
                    }
                    case n => {
                        log.error("INVALID opcode in journal at byte %d: %d", offset, n)
                        throw new IOException("invalid opcode")
                    }
                }
            }
            
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
