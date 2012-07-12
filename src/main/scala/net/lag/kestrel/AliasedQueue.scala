/*
 * Copyright 2012 Twitter, Inc.
 * Copyright 2012 Robey Pointer <robeypointer@gmail.com>
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

import com.twitter.ostrich.stats.Stats
import com.twitter.util.Time
import java.util.concurrent.atomic.AtomicLong
import config._

class AliasedQueue(val name: String, @volatile var config: AliasConfig,
                   queueLookup: (String => Option[PersistentQueue])) {

  def statNamed(statName: String) = "q/" + name + "/" + statName

  // # of items EVER added to the alias:
  val putItems = new AtomicLong(0)
  Stats.removeCounter(statNamed("put_items"))
  Stats.makeCounter(statNamed("put_items"), putItems)

  // # of bytes EVER added to the alias:
  val putBytes = new AtomicLong(0)
  Stats.removeCounter(statNamed("put_bytes"))
  Stats.makeCounter(statNamed("put_bytes"), putBytes)

  val createTime: Long = Time.now.inSeconds
  Stats.addGauge(statNamed("create_time"))(createTime)

  /**
   * Add a value to the end of the aliased queue(s).
   */
  def add(value: Array[Byte], expiry: Option[Time], addTime: Time): Boolean = {
    putItems.getAndIncrement()
    putBytes.getAndAdd(value.length)

    config.destinationQueues.foldLeft(true) { case (result, name) =>
      val thisResult = queueLookup(name) match {
        case Some(q) => q.add(value, expiry, None, addTime)
        case None => true
      }
      result && thisResult
    }
  }

  def dumpStats(): Array[(String, String)] = synchronized {
    Array(
      ("put_items", putItems.toString),
      ("put_bytes", putBytes.toString),
      ("children",  config.destinationQueues.size.toString)
    )
  }
}
