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
import java.util.concurrent.ScheduledThreadPoolExecutor
import scala.util.Sorting
import com.twitter.util.{TempFolder, Time, Timer}
import com.twitter.conversions.time._
import com.twitter.libkestrel.QueueItem
import com.twitter.libkestrel.config._
import com.twitter.ostrich.stats.Stats
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import config._

class QueueCollectionSpec extends Specification
  with TempFolder
  with TestLogging
  with QueueMatchers
  with JMocker
  with ClassMocker
{
  private var qc: QueueCollection = null

  val config = new QueueBuilder { name = "test" }

  "QueueCollection" should {
    val timer = new FakeTimer()
    val scheduler = new ScheduledThreadPoolExecutor(1)

    doAfter {
      if (qc ne null) {
        qc.shutdown()
      }
    }

    "create a queue" in {
      withTempFolder {
        Stats.clearAll()
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil)
        qc.queueNames mustEqual Nil

        qc.add("work1", "stuff".getBytes)
        qc.add("work2", "other stuff".getBytes)

        qc.queueNames.sorted mustEqual List("work1", "work2")
        qc.currentBytes mustEqual 16
        qc.currentItems mustEqual 2
        Stats.getCounter("total_items")() mustEqual 2

        qc.remove("work1")() must beSomeQueueItem("stuff")
        qc.remove("work1")() mustEqual None
        qc.remove("work2")() must beSomeQueueItem("other stuff")
        qc.remove("work2")() mustEqual None

        qc.currentBytes mustEqual 0
        qc.currentItems mustEqual 0
        Stats.getCounter("total_items")() mustEqual 2
      }
    }

    "refuse to create a bad queue" in {
      withTempFolder {
        qc = new QueueCollection(folderName, timer, config, Nil)
        qc.writer("hello.there") must throwA[Exception]
        qc.reader("hello.there") must throwA[Exception]
      }
    }

    "load from journal" in {
      withTempFolder {
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil)
        qc.add("ducklings", "huey".getBytes)
        qc.add("ducklings", "dewey".getBytes)
        qc.add("ducklings", "louie".getBytes)
        qc.queueNames mustEqual List("ducklings")
        qc.currentBytes mustEqual 14
        qc.currentItems mustEqual 3
        qc.shutdown

        qc = new QueueCollection(folderName, timer, scheduler, config, Nil)
        qc.queueNames mustEqual Nil
        qc.remove("ducklings")() must beSomeQueueItem("huey")
        // now the queue should be suddenly instantiated:
        qc.currentBytes mustEqual 10
        qc.currentItems mustEqual 2
      }
    }

    "queue hit/miss tracking" in {
      withTempFolder {
        Stats.clearAll()
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil)
        qc.add("ducklings", "ugly1".getBytes)
        qc.add("ducklings", "ugly2".getBytes)
        Stats.getCounter("get_hits")() mustEqual 0
        Stats.getCounter("get_misses")() mustEqual 0

        qc.remove("ducklings")() must beSomeQueueItem("ugly1")
        Stats.getCounter("get_hits")() mustEqual 1
        Stats.getCounter("get_misses")() mustEqual 0
        qc.remove("zombie")() mustEqual None
        Stats.getCounter("get_hits")() mustEqual 1
        Stats.getCounter("get_misses")() mustEqual 1

        qc.remove("ducklings")() must beSomeQueueItem("ugly2")
        Stats.getCounter("get_hits")() mustEqual 2
        Stats.getCounter("get_misses")() mustEqual 1
        qc.remove("ducklings")() mustEqual None
        Stats.getCounter("get_hits")() mustEqual 2
        Stats.getCounter("get_misses")() mustEqual 2
        qc.remove("ducklings")() mustEqual None
        Stats.getCounter("get_hits")() mustEqual 2
        Stats.getCounter("get_misses")() mustEqual 3
      }
    }

    "proactively load existing queue files" in {
      withTempFolder {
        new File(folderName + "/apples").createNewFile()
        new File(folderName + "/oranges.101").createNewFile()
        new File(folderName + "/oranges.133").createNewFile()
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil)
        qc.loadQueues()
        qc.queueNames.sorted mustEqual List("apples", "oranges")
      }
    }

    "ignore partially rolled queue files" in {
      withTempFolder {
        new File(folderName + "/apples").createNewFile()
        new File(folderName + "/oranges").createNewFile()
        new File(folderName + "/oranges~~900").createNewFile()
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil)
        qc.loadQueues()
        qc.queueNames.sorted mustEqual List("apples", "oranges")
      }
    }

    "delete a queue when asked" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { timeMutator =>
          qc = new QueueCollection(folderName, timer, config, Nil)
          qc.loadQueues()
          qc.writer("apples")
          qc.writer("oranges")
          new File(folderName).list().toList.sorted mustEqual List(
            "apples." + Time.now.inMilliseconds,
            "apples.read.",
            "oranges." + Time.now.inMilliseconds,
            "oranges.read."
          )
          qc.queueNames.sorted mustEqual List("apples", "oranges")

          qc.delete("oranges")
          new File(folderName).list().toList.sorted mustEqual List(
            "apples." + Time.now.inMilliseconds,
            "apples.read."
          )
          qc.queueNames.sorted mustEqual List("apples")
        }
      }
    }

    "fanout queues" in {
      "generate on the fly" in {
        withTempFolder {
          qc = new QueueCollection(folderName, timer, config, Nil)
          qc.reader("jobs+client1")
          qc.add("jobs", "job1".getBytes)
          qc.remove("jobs+client2")() mustEqual None
          qc.add("jobs", "job2".getBytes)

          qc.remove("jobs+client2")() must beSomeQueueItem("job2")
          qc.remove("jobs+client1")() must beSomeQueueItem("job1")
          qc.remove("jobs+client1")() must beSomeQueueItem("job2")
        }
      }

      "preload existing" in {
        withTempFolder {
          val setup = new QueueCollection(folderName, timer, config, Nil)
          setup.loadQueues()
          setup.reader("jobs+client1")
          setup.reader("jobs+client2")
          setup.shutdown()

          qc = new QueueCollection(folderName, timer, config, Nil)
          qc.loadQueues()
          qc.add("jobs", "job1".getBytes)
          qc.remove("jobs+client1")() must beSomeQueueItem("job1")
          qc.add("jobs", "job2".getBytes)

          qc.remove("jobs+client1")() must beSomeQueueItem("job2")
          qc.remove("jobs+client2")() must beSomeQueueItem("job1")
          qc.remove("jobs+client2")() must beSomeQueueItem("job2")
        }
      }

      "delete on the fly" in {
        withTempFolder {
          Time.withCurrentTimeFrozen { _ =>
            val setup = new QueueCollection(folderName, timer, config, Nil)
            setup.loadQueues()
            setup.reader("jobs+client1")
            setup.reader("jobs+client2")
            setup.shutdown()

            qc = new QueueCollection(folderName, timer, config, Nil)
            qc.loadQueues()
            qc.add("jobs", "job1".getBytes)

            qc.delete("jobs+client1")

            new File(folderName).list().toList.sorted mustEqual List(
              "jobs." + Time.now.inMilliseconds,
              "jobs.read.client2"
            )
            qc.remove("jobs+client2")() must beSomeQueueItem("job1")

            qc.add("jobs", "job2".getBytes)
            new File(folderName).list().toList.sorted mustEqual List(
              "jobs." + Time.now.inMilliseconds,
              "jobs.read.client2"
            )
            qc.remove("jobs+client2")() must beSomeQueueItem("job2")
          }
        }
      }
    }

    "expire items when (and only when) they are expired" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          new File(folderName + "/expired").createNewFile()
          qc = new QueueCollection(folderName, timer, scheduler, config, Nil)
          qc.loadQueues()

          qc.add("expired", "hello".getBytes, Some(5.seconds.fromNow))
          time.advance(4.seconds)
          qc.remove("expired")() must beSomeQueueItem("hello")
          qc.add("expired", "hello".getBytes, Some(5.seconds.fromNow))
          time.advance(6.seconds)
          qc.remove("expired")() mustEqual None
        }
      }
    }

    "move expired items from one queue to another" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          new File(folderName + "/jobs").createNewFile()
          new File(folderName + "/expired").createNewFile()
          val expireConfig = new QueueBuilder {
            name = "jobs"
            defaultReader.expireToQueue = "expired"
          }

          qc = new QueueCollection(folderName, timer, config, List(expireConfig))
          qc.loadQueues()
          qc.add("jobs", "hello".getBytes, Some(1.second.fromNow))
          qc.reader("jobs").get.items mustEqual 1
          qc.reader("expired").get.items mustEqual 0

          Kestrel.kestrel = mock[Kestrel]
          expect {
            one(Kestrel.kestrel).queueCollection willReturn qc
          }

          time.advance(2.seconds)
          qc.reader("jobs").get.items mustEqual 1
          qc.reader("expired").get.items mustEqual 0
          qc.remove("jobs")() mustEqual None
          qc.reader("jobs").get.items mustEqual 0
          qc.reader("expired").get.items mustEqual 1
          qc.remove("expired")() must beSomeQueueItem("hello")
        }
      }
    }
  }
}
