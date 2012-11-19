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
import com.twitter.util.{Duration, StorageUnit}

case class QueueConfig(
  maxItems: Int,
  maxSize: StorageUnit,
  maxItemSize: StorageUnit,
  maxAge: Option[Duration],
  defaultJournalSize: StorageUnit,
  maxMemorySize: StorageUnit,
  maxJournalSize: StorageUnit,
  minJournalCompactDelay: Option[Duration],
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
     "maxJournalSize=%s minJournalCompactDelay=%s discardOldWhenFull=%s keepJournal=%s " +
     "syncJournal=%s expireToQueue=%s maxExpireSweep=%d fanoutOnly=%s maxQueueAge=%s").format(
      maxItems, maxSize, maxItemSize, maxAge, defaultJournalSize, maxMemorySize,
      maxJournalSize, minJournalCompactDelay, discardOldWhenFull, keepJournal,
      syncJournal, expireToQueue, maxExpireSweep, fanoutOnly, maxQueueAge)
  }
}

/**
 * QueueBuilder produces QueueConfig objects and can resolve the QueueConfig against
 * a parent QueueConfig, inheriting the parent's values wherever the QueueBuilder's
 * defaults are left unmodified. The default queue (see KestrelConfig) inherits
 * exclusively from the defaults in QueueBuilder. Master queues inherit from the
 * default queue. Fanout queues inherit from their master queue.
 *
 * When constructing a QueueBuilder, values may be overridden as follows:
 * <code>
 *   import com.twitter.conversions.time._
 *
 *   new QueueBuilder() {
 *     name = "the_queue_name"
 *     maxItems = 100
 *     maxAge = 1.day
 *   }
 * </code>
 *
 * Objects are implicitly converted into SpecifiedValue instances. In the case of
 * <code>maxAge</code> the Duration is also wrapped in an option (e.g.,
 * <code>Some(1.day)</code>).
 */
class QueueBuilder {
  import ConfigValue._

  /**
   * Name of the queue being configured.
   */
  var name: String = null

  /**
   * Set a hard limit on the number of items this queue can hold. When the queue is full,
   * `discardOldWhenFull` dictates the behavior when a client attempts to add another item.
   */
  var maxItems: ConfigValue[Int] = Default(Int.MaxValue)

  /**
   * Set a hard limit on the number of bytes (of data in queued items) this queue can hold.
   * When the queue is full, discardOldWhenFull dictates the behavior when a client attempts
   * to add another item.
   */
  var maxSize: ConfigValue[StorageUnit] = Default(Long.MaxValue.bytes)

  /**
   * Set a hard limit on the number of bytes a single queued item can contain.
   * An add request for an item larger than this will be rejected.
   */
  var maxItemSize: ConfigValue[StorageUnit] = Default(Long.MaxValue.bytes)

  /**
   * Expiration time for items on this queue. Any item that has been sitting on the queue longer
   * than this duration will be discarded. Clients may also attach an expiration time when adding
   * items to a queue, but if the expiration time is longer than `maxAge`, `max_Age` will be
   * used instead.
   */
  var maxAge: ConfigValue[Option[Duration]] = Default(None)

  /**
   * If the queue is empty, truncate the journal when it reaches this size.
   */
  var defaultJournalSize: ConfigValue[StorageUnit] = Default(16.megabytes)

  /**
   * Keep only this much of the queue in memory. The journal will be used to store backlogged
   * items, and they'll be read back into memory as the queue is drained. This setting is a release
   * valve to keep a backed-up queue from consuming all memory. Also, when the current journal file
   * reaches this size, it is rotated.
   */
  var maxMemorySize: ConfigValue[StorageUnit] = Default(128.megabytes)

  /**
   * If the queue fits entirely in memory (see maxMemorySize) and the journal files get larger than
   * this, compact the journal. The journal will not be compacted more than once per
   * `minJournalCompactDelay`.
   */
  var maxJournalSize: ConfigValue[StorageUnit] = Default(1.gigabyte)

  /**
   * The minimum amount of time that must pass before consecutive journal compaction operations.
   */
  var minJournalCompactDelay: ConfigValue[Option[Duration]] = Default(Some(1.minute))

  /**
   * If this is false, when a queue is full, clients attempting to add another item will get an
   * error. No new items will be accepted. If this is true, old items will be discarded to make
   * room for the new one. This settting has no effect unless at least one of `maxItems` or
   * `maxSize` is set.
   */
  var discardOldWhenFull: ConfigValue[Boolean] = Default(false)

  /**
   * If false, don't keep a journal file for this queue. When kestrel exits, any remaining contents
   * in the queue will be lost.
   */
  var keepJournal: ConfigValue[Boolean] = Default(true)

  /**
   * How often to sync the journal file. To sync after every write, set this to `0.milliseconds`.
   * To never sync, set it to `Duration.MaxValue`. Syncing the journal will reduce the maximum
   * throughput of the server in exchange for a lower chance of losing data.
   */
  var syncJournal: ConfigValue[Duration] = Default(Duration.MaxValue)

  /**
   * Name of a queue to add expired items to. If set, expired items are added to the requested
   * queue as if by a `SET` command. This can be used to implement special processing for expired
   * items, or to implement a simple "delayed processing" queue.
   */
  var expireToQueue: ConfigValue[Option[String]] = Default(None)

  /**
   * Maximum number of expired items to move into the `expireToQueue` at once.
   */
  var maxExpireSweep: ConfigValue[Int] = Default(Int.MaxValue)

  /**
   * If true, don't actually store any items in this queue. Only deliver them to fanout client
   * queues.
   */
  var fanoutOnly: ConfigValue[Boolean] = Default(false)

  /**
   * Expiration time for the queue itself.  If the queue is empty and older
   * than this value then we should delete it.
   */
  var maxQueueAge: ConfigValue[Option[Duration]] = Default(None)

  def apply(): QueueConfig = apply(None)

  def apply(parent: Option[QueueConfig]) = {
    QueueConfig(maxItems.resolve(parent.map { _.maxItems }),
                maxSize.resolve(parent.map { _.maxSize }),
                maxItemSize.resolve(parent.map { _.maxItemSize }),
                maxAge.resolve(parent.map { _.maxAge }),
                defaultJournalSize.resolve(parent.map { _.defaultJournalSize }),
                maxMemorySize.resolve(parent.map { _.maxMemorySize }),
                maxJournalSize.resolve(parent.map { _.maxJournalSize }),
                minJournalCompactDelay.resolve(parent.map { _.minJournalCompactDelay }),
                discardOldWhenFull.resolve(parent.map { _.discardOldWhenFull }),
                keepJournal.resolve(parent.map { _.keepJournal }),
                syncJournal.resolve(parent.map { _.syncJournal }),
                expireToQueue.resolve(parent.map { _.expireToQueue }),
                maxExpireSweep.resolve(parent.map { _.maxExpireSweep }),
                fanoutOnly.resolve(parent.map { _.fanoutOnly }),
                maxQueueAge.resolve(parent.map { _.maxQueueAge })
              )
  }
}
