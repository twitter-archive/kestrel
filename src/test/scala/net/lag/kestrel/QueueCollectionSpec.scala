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

import _root_.java.io.{File, FileInputStream}
import _root_.scala.util.Sorting
import _root_.com.twitter.xrayspecs.Time
import _root_.com.twitter.xrayspecs.TimeConversions._
import _root_.net.lag.configgy.Config
import _root_.org.specs._


class QueueCollectionSpec extends Specification with TestHelper {

  private var qc: QueueCollection = null

  private def sorted[T <% Ordered[T]](list: List[T]): List[T] = {
    val dest = list.toArray
    Sorting.quickSort(dest)
    dest.toList
  }


  "QueueCollection" should {
    doAfter {
      if (qc ne null) {
        qc.shutdown
      }
    }

    "create a queue" in {
      withTempFolder {
        qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
        qc.queueNames mustEqual Nil

        qc.add("work1", "stuff".getBytes)
        qc.add("work2", "other stuff".getBytes)

        sorted(qc.queueNames) mustEqual List("work1", "work2")
        qc.currentBytes mustEqual 16
        qc.currentItems mustEqual 2
        qc.totalAdded() mustEqual 2

        new String(qc.receive("work1").get) mustEqual "stuff"
        qc.receive("work1") mustEqual None
        new String(qc.receive("work2").get) mustEqual "other stuff"
        qc.receive("work2") mustEqual None

        qc.currentBytes mustEqual 0
        qc.currentItems mustEqual 0
        qc.totalAdded() mustEqual 2
      }
    }

    "load from journal" in {
      withTempFolder {
        qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
        qc.add("ducklings", "huey".getBytes)
        qc.add("ducklings", "dewey".getBytes)
        qc.add("ducklings", "louie".getBytes)
        qc.queueNames mustEqual List("ducklings")
        qc.currentBytes mustEqual 14
        qc.currentItems mustEqual 3
        qc.shutdown

        qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
        qc.queueNames mustEqual Nil
        new String(qc.receive("ducklings").get) mustEqual "huey"
        // now the queue should be suddenly instantiated:
        qc.currentBytes mustEqual 10
        qc.currentItems mustEqual 2
      }
    }

    "queue hit/miss tracking" in {
      withTempFolder {
        qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
        qc.add("ducklings", "ugly1".getBytes)
        qc.add("ducklings", "ugly2".getBytes)
        qc.queueHits() mustEqual 0
        qc.queueMisses() mustEqual 0

        new String(qc.receive("ducklings").get) mustEqual "ugly1"
        qc.queueHits() mustEqual 1
        qc.queueMisses() mustEqual 0
        qc.receive("zombie") mustEqual None
        qc.queueHits() mustEqual 1
        qc.queueMisses() mustEqual 1

        new String(qc.receive("ducklings").get) mustEqual "ugly2"
        qc.queueHits() mustEqual 2
        qc.queueMisses() mustEqual 1
        qc.receive("ducklings") mustEqual None
        qc.queueHits() mustEqual 2
        qc.queueMisses() mustEqual 2
        qc.receive("ducklings") mustEqual None
        qc.queueHits() mustEqual 2
        qc.queueMisses() mustEqual 3
      }
    }

    "proactively load existing queue files" in {
      withTempFolder {
        new File(folderName + "/apples").createNewFile()
        new File(folderName + "/oranges~101").createNewFile()
        new File(folderName + "/oranges~133").createNewFile()
        qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
        qc.loadQueues()
        sorted(qc.queueNames) mustEqual List("apples", "oranges")
      }
    }

    "ignore partially rolled queue files" in {
      withTempFolder {
        new File(folderName + "/apples").createNewFile()
        new File(folderName + "/oranges").createNewFile()
        new File(folderName + "/oranges~~900").createNewFile()
        qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
        qc.loadQueues()
        sorted(qc.queueNames) mustEqual List("apples", "oranges")
      }
    }

    "delete a queue when asked" in {
      withTempFolder {
        new File(folderName + "/apples").createNewFile()
        new File(folderName + "/oranges").createNewFile()
        qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
        qc.loadQueues()
        qc.delete("oranges")

        sorted(new File(folderName).list().toList) mustEqual List("apples")
        sorted(qc.queueNames) mustEqual List("apples")
      }
    }

    "fanout queues" in {
      "generate on the fly" in {
        withTempFolder {
          qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
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
          qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
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
          qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
          qc.loadQueues()
          qc.add("jobs", "job1".getBytes)

          qc.delete("jobs+client1")

          sorted(new File(folderName).list().toList) mustEqual List("jobs")
          new String(qc.receive("jobs").get) mustEqual "job1"

          qc.add("jobs", "job2".getBytes)
          sorted(new File(folderName).list().toList) mustEqual List("jobs")
          new String(qc.receive("jobs").get) mustEqual "job2"
        }
      }
    }

    "move expired items from one queue to another" in {
      withTempFolder {
        new File(folderName + "/jobs").createNewFile()
        new File(folderName + "/expired").createNewFile()
        qc = new QueueCollection(folderName, Config.fromMap(Map("jobs.move_expired_to" -> "expired")))
        Kestrel.queues = qc
        qc.loadQueues()
        qc.add("jobs", "hello".getBytes, 1)
        qc.queue("jobs").get.length mustEqual 1
        qc.queue("expired").get.length mustEqual 0

        Time.advance(1.second)
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
