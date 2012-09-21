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
import com.twitter.logging.{Level, TestLogging}
import com.twitter.util.{TempFolder, Time, Timer}
import com.twitter.conversions.time._
import com.twitter.conversions.storage._
import com.twitter.ostrich.stats.Stats
import org.specs.Specification
import config._

class QueueCollectionSpec extends Specification with TempFolder with TestLogging with QueueMatchers {
  private var qc: QueueCollection = null

  val config = new QueueBuilder().apply()

  "QueueCollection" should {
    val timer = new FakeTimer()
    val scheduler = new ScheduledThreadPoolExecutor(1)

    doAfter {
      if (qc ne null) {
        qc.shutdown
      }
    }

    "create a queue" in {
      withTempFolder {
        Stats.clearAll()
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        qc.queueNames mustEqual Nil
        Stats.getCounter("queue_creates")() mustEqual 0
        Stats.getCounter("queue_deletes")() mustEqual 0

        qc.add("work1", "stuff".getBytes)
        qc.add("work2", "other stuff".getBytes)

        qc.queueNames.sorted mustEqual List("work1", "work2")
        qc.currentBytes mustEqual 16
        qc.currentItems mustEqual 2
        Stats.getCounter("total_items")() mustEqual 2
        Stats.getCounter("queue_creates")() mustEqual 2
        Stats.getCounter("queue_deletes")() mustEqual 0

        qc.remove("work1")() must beSomeQItem("stuff")
        qc.remove("work1")() mustEqual None
        qc.remove("work2")() must beSomeQItem("other stuff")
        qc.remove("work2")() mustEqual None

        qc.currentBytes mustEqual 0
        qc.currentItems mustEqual 0
        Stats.getCounter("total_items")() mustEqual 2
      }
    }

    "refuse to create a bad queue" in {
      withTempFolder {
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        qc.queue("hello.there") must throwA[Exception]
      }
    }

    "refuse to create a bad fanout queue and not break the master queue" in {
      withTempFolder {
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        qc.queue("the_queue")
        qc.queue("the_queue+fanout/open/close") must throwA[Exception]

        qc.add("the_queue", "xypdq".getBytes) mustNot throwA[Exception]
      }
    }

    "report reserved memory usage as a fraction of max heap" in {
      withTempFolder {
        val maxHeapBytes = config.maxMemorySize.inBytes * 4
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil) {
          override lazy val systemMaxHeapBytes = maxHeapBytes
        }

        (1 to 5).foreach { i =>
          qc.queue("queue" + i)

          qc.reservedMemoryRatio mustEqual (i.toDouble / 4.0)
        }
      }
    }

    "load from journal" in {
      withTempFolder {
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        qc.add("ducklings", "huey".getBytes)
        qc.add("ducklings", "dewey".getBytes)
        qc.add("ducklings", "louie".getBytes)
        qc.queueNames mustEqual List("ducklings")
        qc.currentBytes mustEqual 14
        qc.currentItems mustEqual 3
        qc.shutdown

        qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        qc.queueNames mustEqual Nil
        qc.remove("ducklings")() must beSomeQItem("huey")
        // now the queue should be suddenly instantiated:
        qc.currentBytes mustEqual 10
        qc.currentItems mustEqual 2
      }
    }

    "queue hit/miss tracking" in {
      withTempFolder {
        Stats.clearAll()
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        qc.add("ducklings", "ugly1".getBytes)
        qc.add("ducklings", "ugly2".getBytes)
        Stats.getCounter("get_hits")() mustEqual 0
        Stats.getCounter("get_misses")() mustEqual 0

        qc.remove("ducklings")() must beSomeQItem("ugly1")
        Stats.getCounter("get_hits")() mustEqual 1
        Stats.getCounter("get_misses")() mustEqual 0
        qc.remove("zombie")() mustEqual None
        Stats.getCounter("get_hits")() mustEqual 1
        Stats.getCounter("get_misses")() mustEqual 1

        qc.remove("ducklings")() must beSomeQItem("ugly2")
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
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        qc.loadQueues()
        qc.queueNames.sorted mustEqual List("apples", "oranges")
      }
    }

    "ignore partially rolled queue files" in {
      withTempFolder {
        new File(folderName + "/apples").createNewFile()
        new File(folderName + "/oranges").createNewFile()
        new File(folderName + "/oranges~~900").createNewFile()
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        qc.loadQueues()
        qc.queueNames.sorted mustEqual List("apples", "oranges")
      }
    }

    "delete a queue when asked" in {
      withTempFolder {
        new File(folderName + "/apples").createNewFile()
        new File(folderName + "/oranges").createNewFile()
        qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
        Stats.getCounter("queue_deletes")() mustEqual 0
        qc.loadQueues()
        qc.delete("oranges")

        Stats.getCounter("queue_deletes")() mustEqual 1

        new File(folderName).list().toList.sorted mustEqual List("apples")
        qc.queueNames.sorted mustEqual List("apples")
      }
    }

    "fanout queues" in {
      "generate on the fly" in {
        withTempFolder {
          qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
          qc.add("jobs", "job1".getBytes)
          qc.remove("jobs+client1")() mustEqual None
          qc.add("jobs", "job2".getBytes)

          qc.remove("jobs+client1")() must beSomeQItem("job2")
          qc.remove("jobs")() must beSomeQItem("job1")
          qc.remove("jobs")() must beSomeQItem("job2")
        }
      }

      "preload existing" in {
        withTempFolder {
          new File(folderName + "/jobs").createNewFile()
          new File(folderName + "/jobs+client1").createNewFile()
          qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
          qc.loadQueues()
          qc.add("jobs", "job1".getBytes)
          qc.remove("jobs+client1")() must beSomeQItem("job1")
          qc.add("jobs", "job2".getBytes)

          qc.remove("jobs+client1")() must beSomeQItem("job2")
          qc.remove("jobs")() must beSomeQItem("job1")
          qc.remove("jobs")() must beSomeQItem("job2")
        }
      }

      "delete on the fly" in {
        withTempFolder {
          new File(folderName + "/jobs").createNewFile()
          new File(folderName + "/jobs+client1").createNewFile()
          qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
          qc.loadQueues()
          qc.add("jobs", "job1".getBytes)

          qc.delete("jobs+client1")

          new File(folderName).list().toList.sorted mustEqual List("jobs")
          qc.remove("jobs")() must beSomeQItem("job1")

          qc.add("jobs", "job2".getBytes)
          new File(folderName).list().toList.sorted mustEqual List("jobs")
          qc.remove("jobs")() must beSomeQItem("job2")
        }
      }

      "pass through fanout-only master" in {
        withTempFolder {
          new File(folderName + "/jobs+client1").createNewFile()
          val jobConfig = new QueueBuilder() {
            name = "jobs"
            fanoutOnly = true
          }
          qc = new QueueCollection(folderName, timer, scheduler, config, List(jobConfig), Nil)
          qc.loadQueues()
          qc.add("jobs", "job1".getBytes)
          qc.remove("jobs")() mustEqual None
          qc.remove("jobs+client1")() must beSomeQItem("job1")
        }
      }
    }

    "expired items" in {
      "expire items when (and only when) they are expired" in {
        withTempFolder {
          Time.withCurrentTimeFrozen { time =>
            new File(folderName + "/expired").createNewFile()
            qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
            qc.loadQueues()

            qc.add("expired", "hello".getBytes, Some(5.seconds.fromNow))
            time.advance(4.seconds)
            qc.remove("expired")() must beSomeQItem("hello")
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
            val expireConfig = new QueueBuilder() {
              name = "jobs"
              expireToQueue = "expired"
            }
            qc = new QueueCollection(folderName, timer, scheduler, config, List(expireConfig), Nil)
            qc.loadQueues()
            qc.add("jobs", "hello".getBytes, Some(1.second.fromNow))
            qc.queue("jobs").get.length mustEqual 1
            qc.queue("expired").get.length mustEqual 0

            time.advance(2.seconds)
            qc.queue("jobs").get.length mustEqual 1
            qc.queue("expired").get.length mustEqual 0
            qc.remove("jobs")() mustEqual None
            qc.queue("jobs").get.length mustEqual 0
            qc.queue("expired").get.length mustEqual 1
            qc.remove("expired")() must beSomeQItem("hello")
          }
        }
      }
    }

    "aliases" in {
      "alias to a single queue" in {
        withTempFolder {
          val aliasConfig = new AliasBuilder() {
            name = "nom-de-guerre"
            destinationQueues = List("fromage")
          }
          qc = new QueueCollection(folderName, timer, scheduler, config, Nil, List(aliasConfig))
          qc.loadQueues()
          qc.add("nom-de-guerre", "brie".getBytes)
          qc.remove("fromage")() must beSomeQItem("brie")
        }
      }

      "alias to multiple queues" in {
        withTempFolder {
          val aliasConfig = new AliasBuilder() {
            name = "nom-de-guerre"
            destinationQueues = List("fromage", "formaggio")
          }
          qc = new QueueCollection(folderName, timer, scheduler, config, Nil, List(aliasConfig))
          qc.loadQueues()
          qc.add("nom-de-guerre", "brie".getBytes)
          qc.remove("fromage")() must beSomeQItem("brie")
          qc.remove("formaggio")() must beSomeQItem("brie")
        }
      }

      "alias to queues with fanouts" in {
        withTempFolder {
          val queueConfig = List(
            new QueueBuilder() {
              name = "fromage"
              fanoutOnly = true
            },
            new QueueBuilder() {
              name = "formaggio"
              fanoutOnly = true
            })

          val aliasConfig = List(new AliasBuilder() {
            name = "nom-de-guerre"
            destinationQueues = List("fromage", "formaggio")
          })

          qc = new QueueCollection(folderName, timer, scheduler, config, queueConfig, aliasConfig)
          qc.loadQueues()

          // create fanouts
          qc.remove("fromage+brie")() must beNone
          qc.remove("formaggio+gorgonzola")() must beNone

          qc.add("nom-de-guerre", "cheez whiz".getBytes)
          qc.remove("fromage+brie")() must beSomeQItem("cheez whiz")
          qc.remove("formaggio+gorgonzola")() must beSomeQItem("cheez whiz")
        }
      }

      "alias reads always return None" in {
        withTempFolder {
          val aliasConfig = new AliasBuilder() {
            name = "nom-de-guerre"
            destinationQueues = List("fromage")
          }
          qc = new QueueCollection(folderName, timer, scheduler, config, Nil, List(aliasConfig))
          qc.loadQueues()
          qc.add("nom-de-guerre", "brie".getBytes)
          qc.remove("nom-de-guerre")() must beNone
        }
      }

      "log queue name/alias duplicates" in {
        withTempFolder {
          traceLogger(Level.WARNING)

          val queueConfigs = List("q1", "q2", "q3") map { q =>
            new QueueBuilder {
              name = q
            }
          }
          val aliasConfigs = List("a1", "q1", "q2") map { a =>
            new AliasBuilder() {
              name = a
              destinationQueues = List("fromage")
            }
          }

          qc = new QueueCollection(folderName, timer, scheduler, config, queueConfigs, aliasConfigs)
          qc.loadQueues()

          mustLog("queue name(s) masked by alias(es): q1, q2")
        }
      }
    }

    "non-existent queues" in {
      val tests: Map[String, (QueueCollection, String) => Unit] =
        Map("unremove" ->      { (qc: QueueCollection, name: String) => qc.unremove(name, 100) },
            "confirmRemove" -> { (qc: QueueCollection, name: String) => qc.confirmRemove(name, 100) },
            "flush" ->         { (qc: QueueCollection, name: String) => qc.flush(name) },
            "delete" ->        { (qc: QueueCollection, name: String) => qc.delete(name) },
            "flushExpired" ->  { (qc: QueueCollection, name: String) => qc.flushExpired(name, true) },
            "stats" ->         { (qc: QueueCollection, name: String) => qc.stats(name) })
      tests foreach { case (op, test) =>
        "%s should not cause a queue to be created".format(op) in {
          withTempFolder {
            qc = new QueueCollection(folderName, timer, scheduler, config, Nil, Nil)
            test(qc, "some_queue")
            qc.queueNames mustEqual Nil
          }
        }
      }
    }

    "configuration hierarchy" in {
      "default queue should inherit from/override built-in default" in {
        withTempFolder {
          val builtInDefaultConfig = new QueueBuilder().apply()
          val defaultQueueConfig = new QueueBuilder() {
            maxItems = 100
          }.apply()

          qc = new QueueCollection(folderName, timer, scheduler, defaultQueueConfig, Nil, Nil)
          val q = qc.queue("foo").get
          q.config mustEqual builtInDefaultConfig.copy(maxItems = 100)
        }
      }

      "queue should override inherit from/override default queue" in {
        withTempFolder {
          val builtInDefaultConfig = new QueueBuilder().apply()
          val defaultQueueConfig = new QueueBuilder() {
            maxItems = 100
            maxSize = 100.bytes
          }.apply()
          val queueBuilder = new QueueBuilder() {
            name = "foo"
            maxItems = 200
            maxItemSize = 200.bytes
          }

          qc = new QueueCollection(folderName, timer, scheduler, defaultQueueConfig, List(queueBuilder), Nil)
          val q = qc.queue("foo").get
          q.config mustEqual builtInDefaultConfig.copy(maxItems = 200,          // override default queue
                                                       maxSize = 100.bytes,     // inherit default queue
                                                       maxItemSize = 200.bytes) // override built-in
        }
      }

      "fanout queue should inherit from/override master queue" in {
        withTempFolder {
          val builtInDefaultConfig = new QueueBuilder().apply()
          val defaultQueueConfig = new QueueBuilder() {
            maxItems = 100
            maxSize = 100.bytes
            maxJournalSize = 100.bytes
          }.apply()
          val queueBuilders = List(
            new QueueBuilder() {
              name = "foo"
              maxItems = 200
              maxItemSize = 200.bytes
            },
            new QueueBuilder() {
              name = "foo+fanout"
              maxItems = 300
              defaultJournalSize = 300.bytes
              maxJournalSize = 300.bytes
            })

          qc = new QueueCollection(folderName, timer, scheduler, defaultQueueConfig, queueBuilders, Nil)
          val q = qc.queue("foo+fanout").get
          q.config mustEqual builtInDefaultConfig.copy(maxItems = 300,                 // override master queue
                                                       maxItemSize = 200.bytes,        // inherit master queue
                                                       maxJournalSize = 300.bytes,     // override default queue
                                                       maxSize = 100.bytes,            // inherit default queue
                                                       defaultJournalSize = 300.bytes) // override built-in
        }
      }
    }
  }
}
