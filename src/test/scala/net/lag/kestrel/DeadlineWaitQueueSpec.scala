/*
 * Copyright 2012 Twitter, Inc.
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

import com.twitter.conversions.time._
import com.twitter.util.{MockTimer, Time, TimeControl}
import java.util.concurrent.atomic.AtomicInteger
import org.specs.Specification

class DeadlineWaitQueueSpec extends Specification {
  "DeadlineWaitQueue" should {
    val timer = new MockTimer
    val deadlineWaitQueue = new DeadlineWaitQueue(timer)
    var timeouts = new AtomicInteger(0)
    var awakens = new AtomicInteger(0)

    def defaultAwakens() {
      awakens.incrementAndGet()
    }

    def defaultTimeout() {
      timeouts.incrementAndGet()
    }

    "invoke timeout function when deadline expires" in {
      Time.withCurrentTimeFrozen { tc =>
        deadlineWaitQueue.add(10.seconds.fromNow, defaultAwakens, defaultTimeout)

        tc.advance(5.seconds)
        timer.tick()
        timeouts.get mustEqual 0
        awakens.get mustEqual 0

        tc.advance(5.seconds + 1.millisecond)
        timer.tick()
        timeouts.get mustEqual 1
        awakens.get mustEqual 0
      }
    }

    "remove waiters after timeout" in {
      Time.withCurrentTimeFrozen { tc =>
        deadlineWaitQueue.add(10.seconds.fromNow, defaultAwakens, defaultTimeout)
        deadlineWaitQueue.size() mustEqual 1
        tc.advance(11.seconds)
        timer.tick()
        deadlineWaitQueue.size() mustEqual 0
      }
    }

    "invoke the awakens function when triggered" in {
      Time.withCurrentTimeFrozen { tc =>
        deadlineWaitQueue.add(10.seconds.fromNow, defaultAwakens, defaultTimeout)

        tc.advance(5.seconds)
        timer.tick()
        timeouts.get mustEqual 0
        awakens.get mustEqual 0

        deadlineWaitQueue.trigger
        timeouts.get mustEqual 0
        awakens.get mustEqual 1
      }
    }

    "remove waiters after trigger" in {
      Time.withCurrentTimeFrozen { tc =>
        deadlineWaitQueue.add(10.seconds.fromNow, defaultAwakens, defaultTimeout)
        deadlineWaitQueue.size() mustEqual 1
        deadlineWaitQueue.trigger
        deadlineWaitQueue.size() mustEqual 0
      }
    }

    "awaken only a single waiter at a time" in {
      Time.withCurrentTimeFrozen { tc =>
        deadlineWaitQueue.add(10.seconds.fromNow, defaultAwakens, defaultTimeout)
        deadlineWaitQueue.add(10.seconds.fromNow, defaultAwakens, defaultTimeout)
        timeouts.get mustEqual 0
        awakens.get mustEqual 0
        deadlineWaitQueue.size() mustEqual 2

        deadlineWaitQueue.trigger
        timeouts.get mustEqual 0
        awakens.get mustEqual 1
        deadlineWaitQueue.size() mustEqual 1


        deadlineWaitQueue.trigger
        timeouts.get mustEqual 0
        awakens.get mustEqual 2
        deadlineWaitQueue.size() mustEqual 0
      }
    }

    "awaken all waiters when requested" in {
      Time.withCurrentTimeFrozen { tc =>
        deadlineWaitQueue.add(10.seconds.fromNow, defaultAwakens, defaultTimeout)
        deadlineWaitQueue.add(10.seconds.fromNow, defaultAwakens, defaultTimeout)
        timeouts.get mustEqual 0
        awakens.get mustEqual 0

        deadlineWaitQueue.triggerAll
        timeouts.get mustEqual 0
        awakens.get mustEqual 2
      }
    }

    "remove waiters after triggering all" in {
      Time.withCurrentTimeFrozen { tc =>
        deadlineWaitQueue.add(10.seconds.fromNow, defaultAwakens, defaultTimeout)
        deadlineWaitQueue.add(10.seconds.fromNow, defaultAwakens, defaultTimeout)
        deadlineWaitQueue.size() mustEqual 2
        deadlineWaitQueue.triggerAll
        deadlineWaitQueue.size() mustEqual 0
      }
    }

    "explicitly remove a waiter without awakening or timing out" in {
      Time.withCurrentTimeFrozen { tc =>
        val waiter = deadlineWaitQueue.add(10.seconds.fromNow, defaultAwakens, defaultTimeout)
        deadlineWaitQueue.size() mustEqual 1
        timeouts.get mustEqual 0
        awakens.get mustEqual 0
        deadlineWaitQueue.remove(waiter)
        deadlineWaitQueue.size() mustEqual 0
        timeouts.get mustEqual 0
        awakens.get mustEqual 0
      }
    }

    "evict waiters and cancel their timer tasks" in {
      Time.withCurrentTimeFrozen { tc =>
        deadlineWaitQueue.add(10.seconds.fromNow, defaultAwakens, defaultTimeout)
        deadlineWaitQueue.add(10.seconds.fromNow, defaultAwakens, defaultTimeout)
        deadlineWaitQueue.size() mustEqual 2
        timeouts.get mustEqual 0
        awakens.get mustEqual 0

        deadlineWaitQueue.evictAll()
        deadlineWaitQueue.size() mustEqual 0
        timeouts.get mustEqual 2
        awakens.get mustEqual 0

        tc.advance(11.seconds)
        timer.tick()
        deadlineWaitQueue.size() mustEqual 0
        timeouts.get mustEqual 2
        awakens.get mustEqual 0
      }
    }
  }
}
