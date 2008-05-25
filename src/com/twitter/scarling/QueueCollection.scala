package com.twitter.scarling

import java.io.File
import scala.collection.mutable

import net.lag.logging.Logger


class InaccessibleQueuePath extends Exception("Inaccessible queue path")


class QueueCollection(private val queueFolder: String) {
    private val log = Logger.get

    private val path = new File(queueFolder)
    if (! path.isDirectory || ! path.canWrite) {
        throw new InaccessibleQueuePath
    }

    private val queues = new mutable.HashMap[String, PersistentQueue]
    private var shuttingDown = false

    // total of all data in all queues
    private var _currentBytes = 0

    // total of all items in all queues
    private var _currentItems = 0

    // total items added since the server started up.
    private var _totalAdded = 0

    // hits/misses on removing items from the queue
    private var _queueHits = 0
    private var _queueMisses = 0

    // reader accessors:
    def currentBytes = _currentBytes
    def currentItems = _currentItems
    def totalAdded = _totalAdded
    def queueHits = _queueHits
    def queueMisses = _queueMisses


    def queueNames: List[String] = synchronized {
        queues.keys.toList
    }

    /**
     * Get a named queue, creating it if necessary.
     */
    def queue(name: String): Option[PersistentQueue] = {
        var setup = false
        var queue: Option[PersistentQueue] = None

        synchronized {
            if (shuttingDown) {
                return None
            }

            queue = queues.get(name) match {
                case q @ Some(_) => q
                case None => {
                    setup = true
                    val q = new PersistentQueue(path.getPath, name)
                    queues(name) = q
                    Some(q)
                }
            }
        }

        if (setup) {
            /* race is handled by having PersistentQueue start up with an
             * un-initialized flag that blocks all operations until this
             * method is called and completed:
             */
            queue.get.setup
            synchronized {
                _currentBytes += queue.get.bytes
                _currentItems += queue.get.size
            }
        }
        queue
    }

    /**
     * Add an item to a named queue. Will not return until the item has been
     * synchronously added and written to the queue journal file.
     *
     * @return true if the item was added; false if the server is shutting
     *     down
     */
    def add(key: String, item: Array[Byte], expiry: Int): Boolean = {
        queue(key) match {
            case None => false
            case Some(q) => {
                val result = q.add(item, expiry)
                if (result) {
                    synchronized {
                        _currentBytes += item.length
                        _currentItems += 1
                        _totalAdded += 1
                    }
                }
                result
            }
        }
    }

    def add(key: String, item: Array[Byte]): Boolean = add(key, item, 0)

    /**
     * Retrieve an item from a queue. If no item is available, or the server
     * is shutting down, None is returned.
     */
    def remove(key: String): Option[Array[Byte]] = {
        queue(key) match {
            case None => {
                synchronized {
                    _queueMisses += 1
                }
                None
            }
            case Some(q) => {
                val item = q.remove
                synchronized {
                    item match {
                        case None => _queueMisses += 1
                        case Some(x) => {
                            _queueHits += 1
                            _currentBytes -= x.length
                            _currentItems -= 1
                        }
                    }
                }
                item
            }
        }
    }

    def stats(key: String): (Int, Int, Int, Int, Int, Long) = {
        queue(key) match {
            case None => (0, 0, 0, 0, 0, 0)
            case Some(q) => (q.size, q.bytes, q.totalItems, q.journalSize, q.totalExpired, q.currentAge)
        }
    }

    /**
     * Shutdown this queue collection. All actors are asked to exit, and
     * any future queue requests will fail.
     */
    def shutdown: Unit = synchronized {
        if (shuttingDown) {
            return
        }
        shuttingDown = true
        for ((name, q) <- queues) {
            // synchronous, so the journals are all officially closed before we return.
            q.close
        }
        queues.clear
    }
}
