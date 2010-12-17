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

import com.twitter.config.Config
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.util.{Duration, StorageUnit}

case class QueueConfig(
  maxItems: Int,
  maxSize: StorageUnit,
  maxItemSize: StorageUnit,
  maxAge: Option[Duration],
  maxJournalSize: StorageUnit,
  maxMemorySize: StorageUnit,
  maxJournalOverflow: Int,
  discardOldWhenFull: Boolean,
  keepJournal: Boolean,
  syncJournal: Boolean,
  multifileJournal: Boolean,
  expireToQueue: Option[String],
  maxExpireSweep: Int
)

class QueueBuilder extends Config[QueueConfig] {
  var name: String = null
  var maxItems: Int = Int.MaxValue
  var maxSize: StorageUnit = Long.MaxValue.bytes
  var maxItemSize: StorageUnit = Long.MaxValue.bytes
  var maxAge: Option[Duration] = None
  var maxJournalSize: StorageUnit = 16.megabytes
  var maxMemorySize: StorageUnit = 128.megabytes
  var maxJournalOverflow: Int = 10
  var discardOldWhenFull: Boolean = false
  var keepJournal: Boolean = true
  var syncJournal: Boolean = false
  var multifileJournal: Boolean = false
  var expireToQueue: Option[String] = None
  var maxExpireSweep: Int = Int.MaxValue

  def apply() = {
    QueueConfig(maxItems, maxSize, maxItemSize, maxAge, maxJournalSize, maxMemorySize,
                maxJournalOverflow, discardOldWhenFull, keepJournal, syncJournal, multifileJournal,
                expireToQueue, maxExpireSweep)
  }
}

sealed abstract class Protocol
object Protocol {
  case object Ascii extends Protocol
  case object Binary extends Protocol
}

trait KestrelConfig extends Config[Kestrel] {
  /**
   * Settings for a queue that isn't explicitly listed in `queues`.
   */
  val default: QueueBuilder = new QueueBuilder

  /**
   * Specific per-queue config.
   */
  var queues: List[QueueBuilder] = Nil

  /**
   * Maximum threads to allocate to the actor pool.
   * The JVM won't allow anything less than 4.
   */
  var maxThreads: Int = (Runtime.getRuntime().availableProcessors * 2) max 4

  var listenAddress: String = "0.0.0.0"
  var listenPort: Int = 22133
  var queuePath: String = "/tmp"

  /**
   * For future support. Only ascii is supported right now.
   */
  var protocol: Protocol = Protocol.Ascii

  /**
   * If you would like a timer to periodically sweep through queues and clean
   * up expired items (when they are at the head of a queue), set the timer's
   * frequency here. 0 (the default) turns it off. This is only useful for queues
   * that are rarely (or never) polled, but may contain short-lived items.
   */
  var expirationTimerFrequency: Duration = 0.seconds

  var clientTimeout: Duration = 60.seconds

  /**
   * Maximum # of transactions (incomplete GETs) each client can have open at one time.
   */
  var maxOpenTransactions: Int = 1

  def apply(): Kestrel = {
    new Kestrel(default(), queues, maxThreads, listenAddress, listenPort, queuePath,
                protocol, expirationTimerFrequency, clientTimeout, maxOpenTransactions)
  }
}
