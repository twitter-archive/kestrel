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

import com.twitter.common.zookeeper.{ServerSet, ZooKeeperClient, ZooKeeperUtils}
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.logging.config._
import com.twitter.ostrich.admin.{RuntimeEnvironment, ServiceTracker}
import com.twitter.ostrich.admin.config._
import com.twitter.util.{Config, Duration, StorageUnit}
import org.apache.zookeeper.data.ACL
import scala.collection.JavaConversions

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
   * items to a queue, in which case the item expires at the earlier of the two expiration times.
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

case class AliasConfig(
  destinationQueues: List[String]
) {
  override def toString() = {
    ("destinationQueues=[%s]").format(destinationQueues.mkString(", "))
  }
}

class AliasBuilder extends Config[AliasConfig] {
  /**
   * Name of the alias being configured.
   */
  var name: String = null

  /**
   * List of queues which receive items added to this alias.
   */
  var destinationQueues: List[String] = Nil

  def apply() = {
    AliasConfig(destinationQueues)
  }
}

case class ZooKeeperConfig(
  host: String,
  port: Int,
  pathPrefix: String,
  sessionTimeout: Duration,
  credentials: Option[(String, String)],
  acl: ZooKeeperACL,
  clientInitializer: Option[(ZooKeeperConfig) => ZooKeeperClient],
  serverSetInitializer: Option[(ZooKeeperConfig, ZooKeeperClient, String) => ServerSet]) {
  override def toString() = {
    val creds = credentials.map { _ => "<set>" }.getOrElse("<none>")
    val clientInit = clientInitializer.map { _ => "<custom>" }.getOrElse("<default>")
    val serverSetInit = serverSetInitializer.map { _ => "<custom>" }.getOrElse("<default>")

    ("host=%s port=%d pathPrefix=%s sessionTimeout=%s credentials=%s acl=%s " +
     "clientInitializer=%s serverSetInitializer=%s").format(
      host, port, pathPrefix, sessionTimeout, creds, acl, clientInit, serverSetInit)
  }
}

sealed abstract class ZooKeeperACL {
  def asList: List[ACL]
}

/**
 * Predefined, "open" ZooKeeper ACL. This allows any ZooKeeper session to modify or delete the server set and
 * is therefore unsafe for production use. It does, however, work with unauthenticated sessions.
 */
case object OpenUnsafeACL extends ZooKeeperACL {
  def asList = List[ACL]() ++ JavaConversions.asScalaBuffer(ZooKeeperUtils.OPEN_ACL_UNSAFE)
  override def toString() = "<open-unsafe>"
}

/**
 * Predefined ZooKeeper ACL. This allows any ZooKeeper session to read the server set, but only sessions with
 * the creator's credentials may modify it. May only be used with properly configured credentials.
 */
case object EveryoneReadCreatorAllACL extends ZooKeeperACL {
  def asList = List[ACL]() ++ JavaConversions.asScalaBuffer(ZooKeeperUtils.EVERYONE_READ_CREATOR_ALL)
  override def toString() = "<everyone-read/creator-all>"
}

/**
 * Custom ZooKeeper ACL.
 */
case class CustomACL(val asList: List[ACL]) extends ZooKeeperACL {
  override def toString() = "<custom>"
}

class ZooKeeperBuilder {
  /**
   * Hostname for an Apache ZooKeeper cluster to be used for tracking Kestrel
   * server availability. Required.
   *
   * Valid values include "zookeeper.domain.com" and "10.1.2.3".
   */
  var host: String = null

  /**
   * Port for Apache ZooKeeper cluster. Defaults to 2181.
   */
  var port: Int = 2181

  /**
   * Path prefix used to publish Kestrel server availability to the Apache ZooKeeper cluster.
   * Kestrel will append an additional level of hierarchy for the type of operations accepted
   * (e.g., "/read" or "/write"). Required.
   *
   * Example: "/kestrel/production"
   */
  var pathPrefix: String = null

  /**
   * ZooKeeper session timeout. Defaults to 10 seconds.
   */
  var sessionTimeout = 10.seconds

  /**
   * ZooKeeper client connection credentials (username, password). Defaults to unauthenticated
   * connections.
   */
  var credentials: Option[(String, String)] = None

  /**
   * A ZooKeeper ACL to apply to nodes created in the paths created by Kestrel.
   */
  var acl: ZooKeeperACL = OpenUnsafeACL

  /**
   * Overrides ZooKeeperClient intialization. The default implementation uses the configuration
   * options above in a straightforward way. If your environment requires obtaining server set
   * configuration information (e.g., credentials) via another mechanism, you can provide a custom
   * initializer method here. The default implemenation is ZooKeeperServerStatus.createClient.
   *
   * It is strongly recommended that you reference an object method provided in an external JAR
   * rather than placing arbitary code in this configuration file.
   */
  var clientInitializer: Option[(ZooKeeperConfig) => ZooKeeperClient] = None

  /**
   * Overrides ServerSet intialization. The default implementation uses a ZooKeeperClient, the
   * configured pathPrefix and "read" or "write" to produce a ServerSet. The default implementation
   * is ZooKeeperServerStatus.createServerSet.
   *
   * It is strongly recommended that you reference an object method provided in an external JAR
   * rather than placing arbitary code in this configuration file.
   */
  var serverSetInitializer: Option[(ZooKeeperConfig, ZooKeeperClient, String) => ServerSet] = None

  def apply() = {
    ZooKeeperConfig(host, port, pathPrefix, sessionTimeout, credentials, acl, clientInitializer,
                    serverSetInitializer)
  }
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

  /*
   * Alias configurations.
   */
  var aliases: List[AliasBuilder] = Nil

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
   * Port for accepting thrift protocol connections.
   */
  var thriftListenPort: Option[Int] = Some(2229)

  /**
   * Where queue journals should be stored. Each queue will have its own files in this folder.
   */
  var queuePath: String = "/tmp"

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

  /**
   * An optional size for the backlog of connecting clients. This setting is applied to each listening port.
   */
  var connectionBacklog: Option[Int] = None

  /**
   * Path to a file where Kestrel can store information about its current status.
   * When restarted, the server will come up with the same status that it had at shutdown,
   * provided data in this file can be accessed.
   *
   * Kestrel will attempt to create the parent directories of this file if they do not already
   * exist.
   */
  var statusFile: String = "/tmp/.kestrel-status"

  /**
   * In the absence of a readable status file, Kestrel will default to this status.
   */
  var defaultStatus: Status = Up

  /**
   * When changing to a more restricted status (e.g., from Up to ReadOnly), Kestrel will wait
   * until this duration expires before beginning to reject operations. Non-zero settings
   * are useful when using ZooKeeper-based server status. It allows clients to gracefully
   * cease operations without incurring errors.
   */
  var statusChangeGracePeriod = 0.seconds

  /**
   * Optional Apache Zookeeper configuration used to publish serverset-based availability of Kestrel
   * instances. By default no such information is published.
   */
  var zookeeper: Option[ZooKeeperBuilder] = None

  def apply(runtime: RuntimeEnvironment) = {
    new Kestrel(
      default(), queues, aliases, listenAddress, memcacheListenPort, textListenPort, thriftListenPort,
      queuePath, expirationTimerFrequency, clientTimeout, maxOpenTransactions, connectionBacklog,
      statusFile, defaultStatus, statusChangeGracePeriod, zookeeper.map { _() }
    )
  }

  def reload(kestrel: Kestrel) {
    Logger.configure(loggers)
    // only the queue and alias configs can be changed.
    kestrel.reload(default(), queues, aliases)
  }
}
