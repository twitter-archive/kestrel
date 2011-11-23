/*
 * Copyright 2011 Twitter, Inc.
 * Copyright 2011 Robey Pointer <robeypointer@gmail.com>
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
import java.util.concurrent._
import org.specs.Specification
import org.specs.matcher.Matcher

class PeriodicSyncFileSpec extends Specification
  with TestLogging
  with QueueMatchers
{
  "PeriodicSyncTask" should {
    val scheduler = new ScheduledThreadPoolExecutor(4)
    val syncTask = new PeriodicSyncTask(scheduler, 0.seconds, 20.milliseconds)

    doAfter {
      scheduler.shutdown
      scheduler.awaitTermination(5, TimeUnit.SECONDS)
    }

    "only start once" in {
      var invocations = new ConcurrentHashMap[String, Int]()
      syncTask.start { invocations.put("#1", invocations.get("#1") + 1) }
      syncTask.start { invocations.put("#1", invocations.get("#2") + 1) }
      Thread.sleep(100)
      syncTask.stop
      invocations.size mustEqual 1
    }

    "stop" in {
      @volatile var invocations = 0
      syncTask.start { invocations += 1 }
      Thread.sleep(100)
      syncTask.stop
      val invocationsPostTermination = invocations
      Thread.sleep(100)
      invocations mustEqual invocationsPostTermination
    }

    "stop given a condition" in {
      @volatile var invocations = 0
         syncTask.start { invocations += 1 }
      Thread.sleep(100)

      val invocationsPreStop = invocations
      syncTask.stopIf { false }
      Thread.sleep(100)

      val invocationsPostIgnoredStop = invocations
      syncTask.stopIf { true }
      Thread.sleep(100)

      val invocationsPostStop = invocations
      Thread.sleep(100)

      (invocationsPreStop > 0) mustBe true                            // did something
      (invocationsPostIgnoredStop > invocationsPreStop) mustBe true   // kept going
      (invocationsPostStop >= invocationsPostIgnoredStop) mustBe true // maybe did more
      invocations mustEqual invocationsPostStop                       // stopped
    }
  }
}
