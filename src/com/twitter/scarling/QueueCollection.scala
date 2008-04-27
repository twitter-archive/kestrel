package com.twitter.scarling

import java.io.File
import scala.actors.Actor
import scala.actors.Actor._
import scala.collection.mutable

import net.lag.logging.Logger


class InaccessibleQueuePath extends Exception("Inaccessible queue path")


private case class QAdd(item: Array[Byte])
private case object QRemove
private case object QShutdown
private case object QDone


class QueueCollection(private val queueFolder: String) {
    private val log = Logger.get
    
    private val path = new File(queueFolder)
    if (! path.isDirectory || ! path.canWrite) {
        throw new InaccessibleQueuePath
    }
    
    private val queueActors = new mutable.HashMap[String, Actor]
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


    def queues: List[String] = synchronized {
        queueActors.keys.toList
    }
    
    private def addCurrentBytes(n: Int) = synchronized {
        _currentBytes += n
    }
    
    private def addCurrentItems(n: Int) = synchronized {
        _currentItems += n
    }
    
    /**
     * Get a named queue, creating it if necessary.
     */
    private def queue(name: String): Option[Actor] = synchronized {
        if (shuttingDown) {
            return None
        }
        
        queueActors.get(name) match {
            case q @ Some(_) => q
            case None => {
                val qActor = actor {
                    val queue = new PersistentQueue(path.getPath, name)
                    addCurrentBytes(queue.bytes)
                    addCurrentItems(queue.size)
                    
                    loop {
                        react {
                            case QAdd(item) => {
                                queue.add(item)
                                reply(QDone)
                            }
                            case QRemove => reply(queue.remove)
                            case QShutdown => {
                                queue.close
                                reply(QDone)
                                exit()
                            }
                        }
                    }
                }
                queueActors(name) = qActor
                Some(qActor)
            }
        }
    }
    
    /**
     * Add an item to a named queue. Will not return until the item has been
     * synchronously added and written to the queue journal file.
     *
     * @return true if the item was added; false if the server is shutting
     *     down
     */
    def add(key: String, item: Array[Byte]) = {
        queue(key) match {
            case None => false
            case Some(q) => {
                // must be synchronous so we know it's in the queue journal
                q !? new QAdd(item)
                synchronized {
                    _currentBytes += item.length
                    _currentItems += 1
                    _totalAdded += 1
                }
                true
            }
        }
    }
    
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
                val item = (q !? QRemove).asInstanceOf[Option[Array[Byte]]]
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
    
    /**
     * Shutdown this queue collection. All actors are asked to exit, and
     * any future queue requests will fail.
     */
    def shutdown: Unit = synchronized {
        if (shuttingDown) {
            return
        }
        shuttingDown = true
        for ((name, q) <- queueActors) {
            // synchronous, so the journals are all officially closed before we return.
            q !? QShutdown
        }
        queueActors.clear
    }
}
