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
import scala.util.Sorting
import com.twitter.util.{TempFolder, Time, Timer}
import com.twitter.conversions.time._
import com.twitter.ostrich.stats.Stats
import org.specs.Specification
import config._

class QueueCollectionSpec extends Specification with TempFolder with TestLogging {
  private var qc: QueueCollection = null

  val config = new QueueBuilder().apply()

  "QueueCollection" should {
    val timer = new FakeTimer()

    doAfter {
      if (qc ne null) {
        qc.shutdown
      }
    }

    "create a queue" in {
      withTempFolder {
        Stats.clearAll()
        qc = new QueueCollection(folderName, timer, config, Nil)
        qc.queueNames mustEqual Nil

        qc.add("work1", "stuff".getBytes)
        qc.add("work2", "other stuff".getBytes)

        qc.queueNames.sorted mustEqual List("work1", "work2")
        qc.currentBytes mustEqual 16
        qc.currentItems mustEqual 2
        Stats.getCounter("total_items")() mustEqual 2

        new String(qc.receive("work1").get) mustEqual "stuff"
        qc.receive("work1") mustEqual None
        new String(qc.receive("work2").get) mustEqual "other stuff"
        qc.receive("work2") mustEqual None

        qc.currentBytes mustEqual 0
        qc.currentItems mustEqual 0
        Stats.getCounter("total_items")() mustEqual 2
      }
    }

    "load from journal" in {
      withTempFolder {
        qc = new QueueCollection(folderName, timer, config, Nil)
        qc.add("ducklings", "huey".getBytes)
        qc.add("ducklings", "dewey".getBytes)
        qc.add("ducklings", "louie".getBytes)
        qc.queueNames mustEqual List("ducklings")
        qc.currentBytes mustEqual 14
        qc.currentItems mustEqual 3
        qc.shutdown

        qc = new QueueCollection(folderName, timer, config, Nil)
        qc.queueNames mustEqual Nil
        new String(qc.receive("ducklings").get) mustEqual "huey"
        // now the queue should be suddenly instantiated:
        qc.currentBytes mustEqual 10
        qc.currentItems mustEqual 2
      }
    }

    "queue hit/miss tracking" in {
      withTempFolder {
        Stats.clearAll()
        qc = new QueueCollection(folderName, timer, config, Nil)
        qc.add("ducklings", "ugly1".getBytes)
        qc.add("ducklings", "ugly2".getBytes)
        Stats.getCounter("get_hits")() mustEqual 0
        Stats.getCounter("get_misses")() mustEqual 0

        new String(qc.receive("ducklings").get) mustEqual "ugly1"
        Stats.getCounter("get_hits")() mustEqual 1
        Stats.getCounter("get_misses")() mustEqual 0
        qc.receive("zombie") mustEqual None
        Stats.getCounter("get_hits")() mustEqual 1
        Stats.getCounter("get_misses")() mustEqual 1

        new String(qc.receive("ducklings").get) mustEqual "ugly2"
        Stats.getCounter("get_hits")() mustEqual 2
        Stats.getCounter("get_misses")() mustEqual 1
        qc.receive("ducklings") mustEqual None
        Stats.getCounter("get_hits")() mustEqual 2
        Stats.getCounter("get_misses")() mustEqual 2
        qc.receive("ducklings") mustEqual None
        Stats.getCounter("get_hits")() mustEqual 2
        Stats.getCounter("get_misses")() mustEqual 3
      }
    }

    "proactively load existing queue files" in {
      withTempFolder {
        new File(folderName + "/apples").createNewFile()
        new File(folderName + "/oranges.101").createNewFile()
        new File(folderName + "/oranges.133").createNewFile()
        qc = new QueueCollection(folderName, timer, config, Nil)
        qc.loadQueues()
        qc.queueNames.sorted mustEqual List("apples", "oranges")
      }
    }

    "ignore partially rolled queue files" in {
      withTempFolder {
        new File(folderName + "/apples").createNewFile()
        new File(folderName + "/oranges").createNewFile()
        new File(folderName + "/oranges~~900").createNewFile()
        qc = new QueueCollection(folderName, timer, config, Nil)
        qc.loadQueues()
        qc.queueNames.sorted mustEqual List("apples", "oranges")
      }
    }

    "delete a queue when asked" in {
      withTempFolder {
        new File(folderName + "/apples").createNewFile()
        new File(folderName + "/oranges").createNewFile()
        qc = new QueueCollection(folderName, timer, config, Nil)
        qc.loadQueues()
        qc.delete("oranges")

        new File(folderName).list().toList.sorted mustEqual List("apples")
        qc.queueNames.sorted mustEqual List("apples")
      }
    }

    "fanout queues" in {
      "generate on the fly" in {
        withTempFolder {
          qc = new QueueCollection(folderName, timer, config, Nil)
          qc.add("jobs", "job1".getBytes)
          qc.receive("jobs+client1") mustEqual None
          qc.add("jobs", "job2".getBytes)

          new String(qc.receive("jobs+client1").get) mustEqual "job2"
          new String(qc.receive("jobs").get) mustEqual "job1"
          new String(qc.receive("jobs").get) mustEqual "job2"
        }
      }

      "preload existing" in {
        withTempFolder {
          new File(folderName + "/jobs").createNewFile()
          new File(folderName + "/jobs+client1").createNewFile()
          qc = new QueueCollection(folderName, timer, config, Nil)
          qc.loadQueues()
          qc.add("jobs", "job1".getBytes)
          new String(qc.receive("jobs+client1").get) mustEqual "job1"
          qc.add("jobs", "job2".getBytes)

          new String(qc.receive("jobs+client1").get) mustEqual "job2"
          new String(qc.receive("jobs").get) mustEqual "job1"
          new String(qc.receive("jobs").get) mustEqual "job2"
        }
      }

      "delete on the fly" in {
        withTempFolder {
          new File(folderName + "/jobs").createNewFile()
          new File(folderName + "/jobs+client1").createNewFile()
          qc = new QueueCollection(folderName, timer, config, Nil)
          qc.loadQueues()
          qc.add("jobs", "job1".getBytes)

          qc.delete("jobs+client1")

          new File(folderName).list().toList.sorted mustEqual List("jobs")
          new String(qc.receive("jobs").get) mustEqual "job1"

          qc.add("jobs", "job2".getBytes)
          new File(folderName).list().toList.sorted mustEqual List("jobs")
          new String(qc.receive("jobs").get) mustEqual "job2"
        }
      }

      "pass through fanout-only master" in {
        withTempFolder {
          new File(folderName + "/jobs+client1").createNewFile()
          val jobConfig = new QueueBuilder() {
            name = "jobs"
            fanoutOnly = true
          }
          qc = new QueueCollection(folderName, timer, config, List(jobConfig))
          qc.loadQueues()
          qc.add("jobs", "job1".getBytes)
          qc.receive("jobs") mustEqual None
          new String(qc.receive("jobs+client1").get) mustEqual "job1"
        }
      }
    }

    "expire items when (and only when) they are expired" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          new File(folderName + "/expired").createNewFile()
          qc = new QueueCollection(folderName, timer, config, Nil)
          qc.loadQueues()

          qc.add("expired", "hello".getBytes, Some(5.seconds.fromNow))
          time.advance(4.seconds)
          new String(qc.receive("expired").get) mustEqual "hello"
          qc.add("expired", "hello".getBytes, Some(5.seconds.fromNow))
          time.advance(6.seconds)
          qc.receive("expired") mustEqual None
        }
      }
    }

    "move expired items from one queue to another" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          new File(folderName + "/jobs").createNewFile()
          new File(folderName + "/expired").createNewFile()
          val expireConfig = new QueueBuilder() {
            name = "jobs"
            expireToQueue = "expired"
          }
          qc = new QueueCollection(folderName, timer, config, List(expireConfig))
          qc.loadQueues()
          qc.add("jobs", "hello".getBytes, Some(1.second.fromNow))
          qc.queue("jobs").get.length mustEqual 1
          qc.queue("expired").get.length mustEqual 0

          time.advance(2.seconds)
          qc.queue("jobs").get.length mustEqual 1
          qc.queue("expired").get.length mustEqual 0
          qc.receive("jobs") mustEqual None
          qc.queue("jobs").get.length mustEqual 0
          qc.queue("expired").get.length mustEqual 1
          new String(qc.receive("expired").get) mustEqual "hello"
        }
      }
    }
  }
}
