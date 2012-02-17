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
package config

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.logging.config._
import com.twitter.ostrich.admin.{RuntimeEnvironment, ServiceTracker}
import com.twitter.ostrich.admin.config._
import com.twitter.util.{Config, Duration, StorageUnit}

case class QueueConfig(
  maxItems: Int,
  maxSize: StorageUnit,
  maxItemSize: StorageUnit,
  maxAge: Option[Duration],
  defaultJournalSize: StorageUnit,
  maxMemorySize: StorageUnit,
  maxJournalSize: StorageUnit,
  discardOldWhenFull: Boolean,
  keepJournal: Boolean,
  syncJournal: Duration,
  expireToQueue: Option[String],
  maxExpireSweep: Int,
  fanoutOnly: Boolean,
  maxQueueAge: Option[Duration]
) {
  override def toString() = {
    ("maxItems=%d maxSize=%s maxItemSize=%s maxAge=%s defaultJournalSize=%s maxMemorySize=%s " +
     "maxJournalSize=%s discardOldWhenFull=%s keepJournal=%s syncJournal=%s " +
     "expireToQueue=%s maxExpireSweep=%d fanoutOnly=%s maxQueueAge=%s").format(maxItems, maxSize,
     maxItemSize, maxAge, defaultJournalSize, maxMemorySize, maxJournalSize, discardOldWhenFull,
     keepJournal, syncJournal, expireToQueue, maxExpireSweep, fanoutOnly, maxQueueAge)
  }
}

class QueueBuilder extends Config[QueueConfig] {
  /**
   * Name of the queue being configured.
   */
  var name: String = null

  /**
   * Set a hard limit on the number of items this queue can hold. When the queue is full,
   * `discardOldWhenFull` dictates the behavior when a client attempts to add another item.
   */
  var maxItems: Int = Int.MaxValue

  /**
   * Set a hard limit on the number of bytes (of data in queued items) this queue can hold.
   * When the queue is full, discardOldWhenFull dictates the behavior when a client attempts
   * to add another item.
   */
  var maxSize: StorageUnit = Long.MaxValue.bytes

  /**
   * Set a hard limit on the number of bytes a single queued item can contain.
   * An add request for an item larger than this will be rejected.
   */
  var maxItemSize: StorageUnit = Long.MaxValue.bytes

  /**
   * Expiration time for items on this queue. Any item that has been sitting on the queue longer
   * than this duration will be discarded. Clients may also attach an expiration time when adding
   * items to a queue, but if the expiration time is longer than `maxAge`, `max_Age` will be
   * used instead.
   */
  var maxAge: Option[Duration] = None

  /**
   * If the queue is empty, truncate the journal when it reaches this size.
   */
  var defaultJournalSize: StorageUnit = 16.megabytes

  /**
   * Keep only this much of the queue in memory. The journal will be used to store backlogged
   * items, and they'll be read back into memory as the queue is drained. This setting is a release
   * valve to keep a backed-up queue from consuming all memory.
   */
  var maxMemorySize: StorageUnit = 128.megabytes

  /**
   * If the queue fits entirely in memory (see maxMemorySize) and the journal files get larger than
   * this, rebuild the journal.
   */
  var maxJournalSize: StorageUnit = 1.gigabyte

  /**
   * If this is false, when a queue is full, clients attempting to add another item will get an
   * error. No new items will be accepted. If this is true, old items will be discarded to make
   * room for the new one. This settting has no effect unless at least one of `maxItems` or
   * `maxSize` is set.
   */
  var discardOldWhenFull: Boolean = false

  /**
   * If false, don't keep a journal file for this queue. When kestrel exits, any remaining contents
   * in the queue will be lost.
   */
  var keepJournal: Boolean = true

  /**
   * How often to sync the journal file. To sync after every write, set this to `0.milliseconds`.
   * To never sync, set it to `Duration.MaxValue`. Syncing the journal will reduce the maximum
   * throughput of the server in exchange for a lower chance of losing data.
   */
  var syncJournal: Duration = Duration.MaxValue

  /**
   * Name of a queue to add expired items to. If set, expired items are added to the requested
   * queue as if by a `SET` command. This can be used to implement special processing for expired
   * items, or to implement a simple "delayed processing" queue.
   */
  var expireToQueue: Option[String] = None

  /**
   * Maximum number of expired items to move into the `expireToQueue` at once.
   */
  var maxExpireSweep: Int = Int.MaxValue

  /**
   * If true, don't actually store any items in this queue. Only deliver them to fanout client
   * queues.
   */
  var fanoutOnly: Boolean = false

  /**
   * Expiration time for the queue itself.  If the queue is empty and older
   * than this value then we should delete it.
   */
  var maxQueueAge: Option[Duration] = None

  def apply() = {
    QueueConfig(maxItems, maxSize, maxItemSize, maxAge, defaultJournalSize, maxMemorySize,
                maxJournalSize, discardOldWhenFull, keepJournal, syncJournal,
                expireToQueue, maxExpireSweep, fanoutOnly, maxQueueAge)
  }
}

sealed abstract class Protocol
object Protocol {
  case object Ascii extends Protocol
  case object Binary extends Protocol
}

trait KestrelConfig extends ServerConfig[Kestrel] {
  /**
   * Settings for a queue that isn't explicitly listed in `queues`.
   */
  val default: QueueBuilder = new QueueBuilder

  /**
   * Specific per-queue config.
   */
  var queues: List[QueueBuilder] = Nil

  /**
   * Address to listen for client connections. By default, accept from any interface.
   */
  var listenAddress: String = "0.0.0.0"

  /**
   * Port for accepting memcache protocol connections. 22133 is the standard port.
   */
  var memcacheListenPort: Option[Int] = Some(22133)

  /**
   * Port for accepting text protocol connections.
   */
  var textListenPort: Option[Int] = Some(2222)

  /**
   * Where queue journals should be stored. Each queue will have its own files in this folder.
   */
  var queuePath: String = "/tmp"

  /**
   * For future support. Only ascii is supported right now.
   */
  var protocol: Protocol = Protocol.Ascii

  /**
   * If you would like a timer to periodically sweep through queues and clean up expired items
   * (when they are at the head of a queue), set the timer's frequency here. This is only useful
   * for queues that are rarely (or never) polled, but may contain short-lived items.
   */
  var expirationTimerFrequency: Option[Duration] = None

  /**
   * An optional timeout for idle client connections. A client that hasn't sent a request in this
   * period of time will be disconnected.
   */
  var clientTimeout: Option[Duration] = None

  /**
   * Maximum # of transactions (incomplete GETs) each client can have open at one time.
   */
  var maxOpenTransactions: Int = 1

  def apply(runtime: RuntimeEnvironment) = {
    new Kestrel(default(), queues, listenAddress, memcacheListenPort, textListenPort,
                queuePath, protocol, expirationTimerFrequency, clientTimeout,
                maxOpenTransactions)
  }

  def reload(kestrel: Kestrel) {
    Logger.configure(loggers)
    // only the queue configs can be changed.
    kestrel.reload(default(), queues)
  }
}
