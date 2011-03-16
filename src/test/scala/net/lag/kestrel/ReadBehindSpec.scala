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

import java.io.{File, FileInputStream}
import java.util.concurrent.CountDownLatch
import scala.collection.mutable
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.util.{Duration, TempFolder, Time, Timer, TimerTask}
import org.specs.Specification
import config._

class ReadBehindSpec extends Specification with TempFolder with TestLogging with QueueMatchers {

  "PersistentQueue read-behind" should {
    val timer = new FakeTimer()

    "drop into read-behind mode on insert" in {
      withTempFolder {
        val config1 = new QueueBuilder {
          maxMemorySize = 1.kilobyte
        }.apply()
        val q = new PersistentQueue("things", folderName, config1, timer)

        q.setup
        for (i <- 0 until 10) {
          put(q, 128, i)
          q.inReadBehind mustEqual (i >= 8)
        }
        q.inReadBehind mustBe true
        q.length mustEqual 10
        q.bytes mustEqual 1280
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024

        // read 1 item. queue should pro-actively read the next item in from disk.
        q.remove() must beSomeQItem(128, 0)
        q.inReadBehind mustBe true
        q.length mustEqual 9
        q.bytes mustEqual 1152
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024

        // adding a new item should be ok
        put(q, 128, 10)
        q.inReadBehind mustBe true
        q.length mustEqual 10
        q.bytes mustEqual 1280
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024

        // read again.
        q.remove() must beSomeQItem(128, 1)
        q.inReadBehind mustBe true
        q.length mustEqual 9
        q.bytes mustEqual 1152
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024

        // and again.
        q.remove() must beSomeQItem(128, 2)
        q.inReadBehind mustBe true
        q.length mustEqual 8
        q.bytes mustEqual 1024
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024

        for (i <- 3 until 11) {
          q.remove() must beSomeQItem(128, i)
          q.inReadBehind mustBe false
          q.length mustEqual 10 - i
          q.bytes mustEqual 128 * (10 - i)
          q.memoryLength mustEqual 10 - i
          q.memoryBytes mustEqual 128 * (10 - i)
        }
      }
    }

    "drop into read-behind mode on startup" in {
      withTempFolder {
        val config = new QueueBuilder {
          maxMemorySize = 1.kilobyte
        }.apply()
        val q = new PersistentQueue("things", folderName, config, timer)

        q.setup
        for (i <- 0 until 10) {
          put(q, 128, i)
          q.inReadBehind mustEqual (i >= 8)
        }
        q.inReadBehind mustBe true
        q.length mustEqual 10
        q.bytes mustEqual 1280
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024
        q.close

        val q2 = new PersistentQueue("things", folderName, config, timer)
        q2.setup

        q2.inReadBehind mustBe true
        q2.length mustEqual 10
        q2.bytes mustEqual 1280
        q2.memoryLength mustEqual 8
        q2.memoryBytes mustEqual 1024

        for (i <- 0 until 10) {
          q2.remove() must beSomeQItem(128, i)
          q2.inReadBehind mustEqual (i < 2)
          q2.length mustEqual 9 - i
          q2.bytes mustEqual 128 * (9 - i)
          q2.memoryLength mustEqual (if (i < 2) 8 else 9 - i)
          q2.memoryBytes mustEqual (if (i < 2) 1024 else 128 * (9 - i))
        }
      }
    }

    "drop into read-behind mode during journal processing, then return to ordinary times" in {
      withTempFolder {
        val config = new QueueBuilder {
          maxMemorySize = 1.kilobyte
        }.apply()
        val q = new PersistentQueue("things", folderName, config, timer)

        q.setup
        for (i <- 0 until 10) {
          put(q, 128, i)
          q.inReadBehind mustEqual (i >= 8)
        }
        q.inReadBehind mustBe true
        q.length mustEqual 10
        q.bytes mustEqual 1280
        q.memoryLength mustEqual 8
        q.memoryBytes mustEqual 1024
        for (i <- 0 until 10) {
          q.remove() must beSomeQItem(128, i)
        }
        q.inReadBehind mustBe false
        q.length mustEqual 0
        q.bytes mustEqual 0
        q.memoryLength mustEqual 0
        q.memoryBytes mustEqual 0
        q.close

        val q2 = new PersistentQueue("things", folderName, config, timer)
        q2.setup
        q2.inReadBehind mustBe false
        q2.length mustEqual 0
        q2.bytes mustEqual 0
        q2.memoryLength mustEqual 0
        q2.memoryBytes mustEqual 0
      }
    }

    "cope with read-behind on the primary journal file after it gets moved" in {
      withTempFolder {
        val config = new QueueBuilder {
          maxMemorySize = 512.bytes
          maxJournalSize = 1.kilobyte
        }.apply()
        val q = new PersistentQueue("things", folderName, config, timer)

        q.setup
        for (i <- 0 until 10) {
          put(q, 128, i)
          q.inReadBehind mustEqual (i >= 4)
        }
        for (i <- 0 until 10) {
          q.remove() must beSomeQItem(128, i)
        }
        q.close()
      }
    }

    "follow read-behind from several files back" in {
      withTempFolder {
        val config = new QueueBuilder {
          maxMemorySize = 1.kilobyte
          maxJournalSize = 512.bytes
        }.apply()
        val q = new PersistentQueue("things", folderName, config, timer)

        q.setup
        for (i <- 0 until 30) {
          put(q, 128, i)
          q.inReadBehind mustEqual (i >= 8)
        }
        q.inReadBehind mustBe true
        for (i <- 0 until 10) {
          q.remove() must beSomeQItem(128, i)
        }
        q.inReadBehind mustBe true
        for (i <- 30 until 40) {
          put(q, 128, i)
          q.inReadBehind mustBe true
        }
        for (i <- 10 until 40) {
          q.remove() must beSomeQItem(128, i)
        }
        q.inReadBehind mustBe false
        q.close()

        val q2 = new PersistentQueue("things", folderName, config, timer)
        q2.setup
        q2.inReadBehind mustBe false
        q2.length mustEqual 0
        q2.close()
      }
    }
  }
}
