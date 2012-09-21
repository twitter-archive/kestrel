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

import com.twitter.logging.Logger
import com.twitter.logging.config._
import com.twitter.ostrich.admin.{RuntimeEnvironment, ServiceTracker}
import com.twitter.ostrich.admin.config._
import com.twitter.util.{Config, Duration}

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

  def apply(runtime: RuntimeEnvironment) = {
    new Kestrel(
      default(), queues, aliases, listenAddress, memcacheListenPort, textListenPort, thriftListenPort,
      queuePath, expirationTimerFrequency, clientTimeout, maxOpenTransactions, connectionBacklog
    )
  }

  def reload(kestrel: Kestrel) {
    Logger.configure(loggers)
    // only the queue configs can be changed.
    kestrel.reload(default(), queues, aliases)
  }
}
