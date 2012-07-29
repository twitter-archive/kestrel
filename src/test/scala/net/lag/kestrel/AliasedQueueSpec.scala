/*
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

import java.util.concurrent.ScheduledThreadPoolExecutor
import com.twitter.conversions.time._
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{TempFolder, Time}
import org.specs.Specification
import config._

class AliasedQueueSpec extends Specification with TempFolder {

  private var qc: QueueCollection = null
  private var aq: AliasedQueue = null

  val queueConfig = new QueueBuilder().apply()
  val aliasConfig = new AliasBuilder().apply()

  "AliasedQueue" should {
    val timer = new FakeTimer()
    val scheduler = new ScheduledThreadPoolExecutor(1)

    doBefore {
      withTempFolder {
	Stats.clearAll()
	qc = new QueueCollection(folderName, timer, scheduler, queueConfig, Nil, Nil)
	aq = new AliasedQueue("kestrel", aliasConfig, qc)
      }
    }

    doAfter {
      if (qc ne null) {
	qc.shutdown
      }
    }

    "generate a stats queue name" in {
      aq.statNamed("queue") mustEqual "q/kestrel/queue"
    }

    "add a value to the end of an aliased queue" in {
      aq.add(Array(1, 2, 3, 4), None, Time.now) mustEqual true
    }

    "return an array of empty stats when no value was added to the queue" in {
      val stats: Array[(String, String)] = aq.dumpStats()

      for (i <- 0 to 2) i match {
	case 0 => {
	  val (key, value) = stats(i)
	  key mustEqual "put_items"
	  value mustEqual "0"
	}

	case 1 => {
	  val (key, value) = stats(i)
	  key mustEqual "put_bytes"
	  value mustEqual "0"
	}

	case 2 => {
	  val (key, value) = stats(i)
	  key mustEqual "children"
	  value mustEqual ""
	}
      }
    }

    "return an array of stats when a value is added to the aliased queue" in {
      aq.add(Array(1, 2, 3, 4), None, Time.now)
      val stats: Array[(String, String)] = aq.dumpStats()

      for (i <- 0 to 2) i match {
	case 0 => {
	  val (key, value) = stats(i)
	  key mustEqual "put_items"
	  value mustEqual "1"
	}

	case 1 => {
	  val (key, value) = stats(i)
	  key mustEqual "put_bytes"
	  value mustEqual "4"
	}

	case 2 => {
	  val (key, value) = stats(i)
	  key mustEqual "children"
	  value mustEqual ""
	}
      }
    }
  }
}
