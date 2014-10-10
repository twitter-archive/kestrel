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
import com.twitter.util.Duration
import org.apache.zookeeper.data.ACL
import scala.collection.JavaConversions

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
   * configured pathPrefix and the node type (the third argument to this function; always "read" or
   * "write") to produce a ServerSet.  The default implementation is
   * ZooKeeperServerStatus.createServerSet.
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

/**
 * KestrelConfig is the main point of configuration for Kestrel.
 */
trait KestrelConfig extends ServerConfig[Kestrel] {
  /**
   * Default queue settings. Starting with Kestrel 2.3.4, queue settings are
   * inherited. See QueueBuilder for more information.
   */
  val default: QueueBuilder = new QueueBuilder

  /**
   * Specific per-queue config. Starting with Kestrel 2.3.4, queue settings are
   * inherited. See QueueBuilder for more information.
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
   * When true, enables tracing of session lifetime in the kestrel log
   */
  var enableSessionTrace: Boolean = false

  /**
   * When set refuses write after the specified number of connections
   */
  var connectionLimitRefuseWrites: Option[Int] = None

  /**
   * When set refuses reads after the specified number of connections
   */
  var connectionLimitRefuseReads: Option[Int] = None

  /**
   * Optional Apache Zookeeper configuration used to publish serverset-based availability of Kestrel
   * instances. By default no such information is published.
   */
  var zookeeper: Option[ZooKeeperBuilder] = None

  /**
   * Fully qualified class name for extension backend.
   */
  var beFactoryClass: Option[String] = None

  def apply(runtime: RuntimeEnvironment) = {
    new Kestrel(
      default(), queues, aliases, listenAddress, memcacheListenPort, textListenPort, thriftListenPort,
      queuePath, expirationTimerFrequency, clientTimeout, maxOpenTransactions, connectionBacklog,
      statusFile, defaultStatus, statusChangeGracePeriod, enableSessionTrace,
      connectionLimitRefuseWrites, connectionLimitRefuseReads, zookeeper.map { _()}, beFactoryClass
    )
  }

  def reload(kestrel: Kestrel) {
    Logger.configure(loggers)
    // only the queue and alias configs can be changed.
    kestrel.reload(default(), queues, aliases)
  }
}
