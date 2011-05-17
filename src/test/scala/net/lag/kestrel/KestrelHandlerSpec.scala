/*
 * Copyright 2010 Twitter, Inc.
 * Copyright 2010 Robey Pointer <robeypointer@gmail.com>
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
import com.twitter.conversions.time._
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{TempFolder, Time, Timer}
import org.specs.Specification
import org.specs.matcher.Matcher
import config._

class FakeKestrelHandler(queues: QueueCollection, maxOpenTransactions: Int)
      extends KestrelHandler(queues, maxOpenTransactions) {
  protected def clientDescription: String = "none"
}

class KestrelHandlerSpec extends Specification with TempFolder with TestLogging {
  val config = new QueueBuilder().apply()

  case class beString(expected: String) extends Matcher[Option[QItem]]() {
    def apply(v: => Option[QItem]) = {
      val actual = v.map { item => new String(item.data) }
      (actual == Some(expected), "ok", "item " + actual + " != " + expected)
    }
  }

  "KestrelHandler" should {
    var queues: QueueCollection = null
    val timer = new FakeTimer()

    doAfter {
      queues.shutdown()
    }

    "set and get" in {
      withTempFolder {
        queues = new QueueCollection(folderName, timer, config, Nil)
        val handler = new FakeKestrelHandler(queues, 10)
        handler.setItem("test", 0, None, "one".getBytes)
        handler.setItem("test", 0, None, "two".getBytes)
        handler.getItem("test", None, false, false).get() must beString("one")
        handler.getItem("test", None, false, false).get() must beString("two")
      }
    }

    "track stats" in {
      withTempFolder {
        Stats.clearAll()
        queues = new QueueCollection(folderName, timer, config, Nil)
        val handler = new FakeKestrelHandler(queues, 10)

        Stats.getCounter("cmd_get")() mustEqual 0
        Stats.getCounter("cmd_set")() mustEqual 0
        Stats.getCounter("get_hits")() mustEqual 0
        Stats.getCounter("get_misses")() mustEqual 0

        handler.setItem("test", 0, None, "one".getBytes)
        Stats.getCounter("cmd_set")() mustEqual 1
        Stats.getCounter("cmd_get")() mustEqual 0

        handler.getItem("test", None, false, false).get() must beString("one")
        Stats.getCounter("cmd_set")() mustEqual 1
        Stats.getCounter("cmd_get")() mustEqual 1
        Stats.getCounter("get_hits")() mustEqual 1
        Stats.getCounter("get_misses")() mustEqual 0

        handler.getItem("test2", None, false, false).get() mustEqual None
        Stats.getCounter("cmd_set")() mustEqual 1
        Stats.getCounter("cmd_get")() mustEqual 2
        Stats.getCounter("get_hits")() mustEqual 1
        Stats.getCounter("get_misses")() mustEqual 1
      }
    }
/*
    "abort and confirm a transaction" in {
      withTempFolder {
        queues = new QueueCollection(folderName, timer, config, Nil)
        val handler = new FakeKestrelHandler(queues, 10)
        handler.setItem("test", 0, None, "one".getBytes)
        handler.getItem("test", None, true, false) { _ must beString("one") }
        handler.getItem("test", None, true, false) { _ mustEqual None }
        handler.abortTransaction("test") mustEqual true
        handler.getItem("test", None, true, false) { _ must beString("one") }
        handler.closeTransaction("test") mustEqual true
        handler.getItem("test", None, true, false) { _ mustEqual None }
      }
    }

    "open several transactions" in {
      "on one queue" in {
        withTempFolder {
          queues = new QueueCollection(folderName, timer, config, Nil)
          val handler = new FakeKestrelHandler(queues, 10)
          handler.setItem("test", 0, None, "one".getBytes)
          handler.setItem("test", 0, None, "two".getBytes)
          handler.setItem("test", 0, None, "three".getBytes)
          handler.getItem("test", None, true, false) { _ must beString("one") }
          handler.getItem("test", None, true, false) { _ must beString("two") }
          handler.abortTransaction("test") mustEqual true
          handler.getItem("test", None, true, false) { _ must beString("one") }
          handler.closeTransaction("test") mustEqual true
          handler.getItem("test", None, true, false) { _ must beString("three") }
          handler.abortTransaction("test") mustEqual true
          handler.getItem("test", None, true, false) { _ must beString("one") }
        }
      }

      "on several queues" in {
        withTempFolder {
          queues = new QueueCollection(folderName, timer, config, Nil)
          val handler = new FakeKestrelHandler(queues, 10)
          handler.setItem("red", 0, None, "red1".getBytes)
          handler.setItem("red", 0, None, "red2".getBytes)
          handler.setItem("green", 0, None, "green1".getBytes)
          handler.setItem("green", 0, None, "green2".getBytes)
          handler.setItem("blue", 0, None, "blue1".getBytes)
          handler.setItem("blue", 0, None, "blue2".getBytes)

          handler.getItem("red", None, true, false) { _ must beString("red1") }
          handler.getItem("green", None, true, false) { _ must beString("green1") }
          handler.getItem("blue", None, true, false) { _ must beString("blue1") }
          handler.abortTransaction("green") mustEqual true

          handler.getItem("red", None, true, false) { _ must beString("red2") }
          handler.closeTransaction("red") mustEqual true
          handler.closeTransaction("red") mustEqual true
          handler.getItem("red", None, true, false) { _ mustEqual None }

          handler.getItem("green", None, true, false) { _ must beString("green1") }
          handler.closeTransaction("blue") mustEqual true
          handler.abortTransaction("green") mustEqual true
          handler.getItem("blue", None, true, false) { _ must beString("blue2") }
          handler.getItem("green", None, true, false) { _ must beString("green1") }
        }
      }

      "but not if transactions are limited" in {
        withTempFolder {
          queues = new QueueCollection(folderName, timer, config, Nil)
          val handler = new FakeKestrelHandler(queues, 1)
          handler.setItem("red", 0, None, "red1".getBytes)
          handler.setItem("red", 0, None, "red2".getBytes)
          handler.getItem("red", None, true, false) { _ must beString("red1") }
          handler.getItem("red", None, true, false) { x => x } must throwA[TooManyOpenTransactionsException]
        }
      }

      "close all transactions" in {
        withTempFolder {
          queues = new QueueCollection(folderName, timer, config, Nil)
          val handler = new FakeKestrelHandler(queues, 2)
          handler.setItem("red", 0, None, "red1".getBytes)
          handler.setItem("red", 0, None, "red2".getBytes)
          handler.getItem("red", None, true, false) { _ must beString("red1") }
          handler.getItem("red", None, true, false) { _ must beString("red2") }
          handler.closeAllTransactions("red") mustEqual 2
          handler.abortTransaction("red") mustEqual false
          handler.pendingTransactions.size("red") mustEqual 0
        }
      }
    }
    */
  }
}
