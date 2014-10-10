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
import java.util.concurrent.{CountDownLatch, ScheduledThreadPoolExecutor}
import scala.collection.mutable
import scala.util.Random
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.ostrich.stats.Stats
import com.twitter.logging.TestLogging
import com.twitter.util.{Await, Duration, TempFolder, Time, Timer, TimerTask}
import org.specs.SpecificationWithJUnit
import org.specs.matcher.Matcher
import config._

class PersistentQueueSpec extends SpecificationWithJUnit
  with TempFolder
  with TestLogging
  with QueueMatchers
  with DumpJournal
{
  "PersistentQueue" should {
    val timer = new FakeTimer()
    val scheduler = new ScheduledThreadPoolExecutor(1)

    def withJournalPacker(f: => Unit) {
      Journal.packer.start()
      try {
        f
      } finally {
        Journal.packer.shutdown()
      }
    }

    def interruptRewrites(name: String, failpoint: Failpoint): String = {
      val q = new PersistentQueue(name, folderName, new QueueBuilder().apply(), timer, scheduler)
      q.setup()
      q.add("zero".getBytes)
      q.add("first".getBytes)
      q.add("second".getBytes)

      // force-bump xid
      val zero = q.remove(true).get
      new String(zero.data) mustEqual "zero"
      q.confirmRemove(zero.xid)

      val item = q.remove(true).get
      new String(item.data) mustEqual "first"
      q.forceRewrite(failpoint)
      q.close()

      val q2 = new PersistentQueue(name, folderName, new QueueBuilder().apply(), timer, scheduler)
      q2.setup()
      q2.length mustEqual 2
      new String(q2.remove().get.data) mustEqual "first"
      new String(q2.remove().get.data) mustEqual "second"
      q2.close()
      dumpJournal(name)
    }

    def verifyQLengthAndDumpJournal (q: PersistentQueue, queueName: String, expectedLength: Int) {
      if (q.length != expectedLength) {
        // If the queue length doesn't meet the expectations, dump the journal so
        // that we can debug the cause of the incorrect length
        (q.length, dumpJournal(queueName)) mustEqual (expectedLength, "")
      }
    }

    doBefore {
      timer.timerTask.cancelled = false
    }

    "add and remove one item" in {
      withTempFolder {
        val q = new PersistentQueue("work", folderName, new QueueBuilder().apply(), timer, scheduler)
        q.setup

        q.length mustEqual 0
        q.putItems.get mustEqual 0L
        q.bytes mustEqual 0
        q.journalSize mustEqual 0

        q.add("hello kitty".getBytes)

        q.length mustEqual 1
        q.putItems.get mustEqual 1L
        q.putBytes.get mustEqual 11L
        q.bytes mustEqual 11
        q.journalSize mustEqual 32
        new File(folderName, "work").length mustEqual 32

        new String(q.remove.get.data) mustEqual "hello kitty"

        q.length mustEqual 0
        q.putItems.get mustEqual 1L
        q.putBytes.get mustEqual 11L
        q.bytes mustEqual 0
        q.journalSize mustEqual 33

        q.close
        dumpJournal("work") mustEqual "add(11:0:hello kitty), remove"
      }
    }

    "resist adding an item that's too large" in {
      withTempFolder {
        val config = new QueueBuilder {
          maxItemSize = 128.bytes
        }.apply()
        val q = new PersistentQueue("work", folderName, config, timer, scheduler)
        q.setup()
        q.length mustEqual 0
        q.add(new Array[Byte](127)) mustEqual true
        q.add(new Array[Byte](128)) mustEqual true
        q.add(new Array[Byte](129)) mustEqual false
        q.close
      }
    }

    "flush all items" in {
      withTempFolder {
        val q = new PersistentQueue("work", folderName, new QueueBuilder().apply(), timer, scheduler)
        q.setup()

        q.length mustEqual 0
        q.putItems.get mustEqual 0L
        q.bytes mustEqual 0
        q.journalSize mustEqual 0
        q.totalFlushes() mustEqual 0

        q.add("alpha".getBytes)
        q.add("beta".getBytes)
        q.add("gamma".getBytes)
        q.length mustEqual 3

        q.flush()
        q.length mustEqual 0

        q.totalFlushes() mustEqual 1

        // journal should contain exactly: one unfinished transaction, 2 items.
        q.close
        dumpJournal("work") mustEqual
          "add(5:0:alpha), add(4:0:beta), add(5:0:gamma), remove, remove, remove"
      }
    }

    "rotate journals when they exceed maxMemorySize" in {
      withTempFolder {
        val config = new QueueBuilder {
          maxMemorySize = 64.bytes
        }.apply()
        val q = new PersistentQueue("rotating", folderName, config, timer, scheduler)
        q.setup()

        q.add(new Array[Byte](32))
        JournalTestUtil.journalsForQueue(new File(folderName), "rotating").length mustEqual 1

        q.add(new Array[Byte](32))
        JournalTestUtil.journalsForQueue(new File(folderName), "rotating").length mustEqual 1

        q.add(new Array[Byte](32))
        JournalTestUtil.journalsForQueue(new File(folderName), "rotating").length mustEqual 2
      }
    }

    "rotate and pack journals" in {
      withTempFolder {
        withJournalPacker {
          val config = new QueueBuilder {
            defaultJournalSize = 16.bytes
            maxJournalSize = 256.bytes
            maxMemorySize = 64.bytes
          }.apply()
          val queueName = "rotate-pack-journal" + Random.nextInt
          val q = new PersistentQueue(queueName, folderName, config, timer, scheduler)
          q.setup()

          // Set the checkpoint
          (1 to 16).foreach { _ =>
            q.add(new Array[Byte](32))
          }
          q.length mustEqual 16

          (1 to 15).foreach { _ =>
            q.remove
            q.add(new Array[Byte](32))
          }
          q.length mustEqual 16
          q.close()

          val q2 = new PersistentQueue(queueName, folderName, config, timer, scheduler)
          q2.setup()
          verifyQLengthAndDumpJournal(q2, queueName, 16)
        }
      }
    }

    "rotate and pack journals and then recover" in {
      withTempFolder {
        withJournalPacker {
          val config = new QueueBuilder {
            defaultJournalSize = 16.bytes
            maxJournalSize = 256.bytes
            maxMemorySize = 64.bytes
          }.apply()
          val queueName = "rotate-pack-journal-recover" + Random.nextInt
          val q = new PersistentQueue(queueName, folderName, config, timer, scheduler)
          q.setup()

          // Set the checkpoint
          (1 to 16).foreach { _ =>
            q.add(new Array[Byte](32))
          }
          q.length mustEqual 16

          (1 to 15).foreach { _ =>
            q.remove
            q.add(new Array[Byte](32))
          }
          q.length mustEqual 16
          q.close()

          val q2 = new PersistentQueue(queueName, folderName, config, timer, scheduler)
          q2.setup()
          verifyQLengthAndDumpJournal(q2, queueName, 16)

          (1 to 31).foreach { _ =>
            q2.remove
            q2.add(new Array[Byte](32))
          }
          q2.length mustEqual 16
          q2.close()

          val q3 = new PersistentQueue(queueName, folderName, config, timer, scheduler)
          q3.setup()
          verifyQLengthAndDumpJournal(q3, queueName, 16)
        }
      }
    }

    "rotate pack journals with remove tentative and then recover" in {
      withTempFolder {
        withJournalPacker {
          val config = new QueueBuilder {
            defaultJournalSize = 16.bytes
            maxJournalSize = 256.bytes
            maxMemorySize = 64.bytes
          }.apply()
          val queueName = "rotate-pack-journal-recover-tentative" + Random.nextInt
          val q = new PersistentQueue(queueName, folderName, config, timer, scheduler)
          q.setup()

          // Set the checkpoint
          (1 to 16).foreach { _ =>
            q.add(new Array[Byte](32))
          }
          q.length mustEqual 16

          (1 to 10).foreach { _ =>
            val item = q.remove(true).get
            q.confirmRemove(item.xid)
            q.add(new Array[Byte](32))
          }
          q.length mustEqual 16
          q.close()

          val q2 = new PersistentQueue(queueName, folderName, config, timer, scheduler)
          q2.setup()
          verifyQLengthAndDumpJournal(q2, queueName, 16)

          (1 to 31).foreach { _ =>
            q2.remove
            q2.add(new Array[Byte](32))
          }
          q2.length mustEqual 16
          q2.close()

          val q3 = new PersistentQueue(queueName, folderName, config, timer, scheduler)
          q3.setup()
          verifyQLengthAndDumpJournal(q3, queueName, 16)
        }
      }
    }

    "rewrite journals when they exceed the defaultJournalSize and are empty" in {
      withTempFolder {
        val config = new QueueBuilder {
          defaultJournalSize = 64.bytes
        }.apply()
        val q = new PersistentQueue("rewriting", folderName, config, timer, scheduler)
        q.setup()

        q.add(new Array[Byte](32))
        q.add(new Array[Byte](64))
        q.length mustEqual 2
        q.putItems.get mustEqual 2L
        q.bytes mustEqual 32 + 64
        (q.journalTotalSize > 96) mustBe true

        q.remove()
        q.length mustEqual 1
        q.putItems.get mustEqual 2L
        q.bytes mustEqual 64
        (q.journalTotalSize > 96) mustBe true

        // now it should rotate:
        q.remove()
        q.length mustEqual 0
        q.putItems.get mustEqual 2L
        q.bytes mustEqual 0
        (q.journalTotalSize < 10) mustBe true
      }
    }

    "rewrite journals with an open transaction" in {
      withTempFolder {
        val config = new QueueBuilder {
          defaultJournalSize = 64.bytes
        }.apply()
        val q = new PersistentQueue("rewriting", folderName, config, timer, scheduler)
        q.setup()

        q.add(new Array[Byte](32))
        q.add(new Array[Byte](64))
        q.length mustEqual 2
        q.putItems.get mustEqual 2
        q.bytes mustEqual 32 + 64
        (q.journalSize > 96) mustBe true

        q.remove()
        q.length mustEqual 1
        q.putItems.get mustEqual 2
        q.bytes mustEqual 64
        (q.journalSize > 96) mustBe true

        // now it should rotate:
        q.remove(true)
        q.length mustEqual 0
        q.openTransactionCount mustEqual 1
        q.putItems.get mustEqual 2
        q.bytes mustEqual 0
        (q.journalSize < 96) mustBe true
      }
    }

    "timeout open transactions" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          val config = new QueueBuilder {
            openTransactionTimeout = 10.milliseconds
          }.apply()
          val q = new PersistentQueue("rewriting", folderName, config, timer, scheduler)
          q.setup

          q.add("one".getBytes)
          q.add("two".getBytes)
          q.add("three".getBytes)
          q.add("four".getBytes)
          q.add("five".getBytes)

          q.length mustEqual 5
          q.putItems.get mustEqual 5

          val item1 = q.remove(true)
          item1 must beSome[QItem].which { item => new String(item.data) == "one" }
          new String(item1.get.data) mustEqual "one"
          val item2 = q.remove(true)
          new String(item2.get.data) mustEqual "two"
          val item3 = q.remove(true)
          new String(item3.get.data) mustEqual "three"

          time.advance(6.milliseconds)

          val item4 = q.remove(true)
          new String(item4.get.data) mustEqual "four"

          time.advance(6.milliseconds)

          val item5 = q.remove(true)
          new String(item5.get.data) mustEqual "one"
          val item6 = q.remove(true)
          new String(item6.get.data) mustEqual "two"
          val item7 = q.remove(true)
          new String(item7.get.data) mustEqual "three"
        }
      }
    }


    "rewrite journals when they exceed the maxJournalSize but still fit in memory" in {
      withTempFolder {
        val config = new QueueBuilder {
          defaultJournalSize = 64.bytes
          maxJournalSize = 128.bytes
        }.apply()
        val q = new PersistentQueue("rewriting", folderName, config, timer, scheduler)
        q.setup()

        q.add(new Array[Byte](64))
        q.add(new Array[Byte](64))

        q.remove(false)
        q.length mustEqual 1
        (q.journalSize < 128) mustBe true
      }
    }

    "not rewrite journals repeatedly on add" in {
      withTempFolder {
        val config = new QueueBuilder {
          defaultJournalSize = 16.bytes
          maxJournalSize = 160.bytes
          maxMemorySize = 128.bytes
        }.apply()
        val q = new PersistentQueue("rewriting", folderName, config, timer, scheduler)
        q.setup()

        // serialized QItem = 1 + 21 bytes, 8 are 176.bytes:
        (1 to 8).foreach { _ => q.add(new Array[Byte](1)) }

        q.add(new Array[Byte](1))
        q.totalRewrites() mustEqual 1

        q.add(new Array[Byte](1))
        q.totalRewrites() mustEqual 1
      }
    }

    "allow rewrites after some time" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          val config = new QueueBuilder {
            defaultJournalSize = 16.bytes
            maxJournalSize = 160.bytes
            maxMemorySize = 128.bytes
            minJournalCompactDelay = 5.seconds
          }.apply()
          val q = new PersistentQueue("rewriting", folderName, config, timer, scheduler)
          q.setup()

          // serialized QItem = 1 + 21 bytes, 8 are 176.bytes:
          (1 to 8).foreach { _ => q.add(new Array[Byte](1)) }

          q.add(new Array[Byte](1))
          q.totalRewrites() mustEqual 1

          q.add(new Array[Byte](1))
          q.totalRewrites() mustEqual 1

          time.advance(5.seconds)
          timer.timeout()

          q.add(new Array[Byte](1))
          q.totalRewrites() mustEqual 2
        }
      }
    }

    "allow rewrites for empty queue after some time" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          val config = new QueueBuilder {
            disableAggressiveRewrites = true
            defaultJournalSize = 160.bytes
            maxJournalSize = 1024.bytes
            maxMemorySize = 1024.bytes
            minJournalCompactDelay = 5.seconds
          }.apply()
          val q = new PersistentQueue("aggressiverewritingemptyqueueopentran", folderName, config, timer, scheduler)
          q.setup()

          // Leave 8 open transactions so that the journal rewrites
          // results in no shrinkage
          (1 to 8).foreach { _ =>
            q.add(new Array[Byte](1))
            q.remove(true)
          }

          q.totalRewrites() mustEqual 1

          (1 to 8).foreach { _ =>
            q.add(new Array[Byte](1))
            q.remove
          }
          q.totalRewrites() mustEqual 1

          time.advance(5.seconds)
          timer.timeout()

          q.add(new Array[Byte](1))
          q.remove
          q.totalRewrites() mustEqual 2
        }
      }
    }

    "continue aggressive rewrites for empty queue if rewrite frees up space" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          val config = new QueueBuilder {
            defaultJournalSize = 160.bytes
            maxJournalSize = 1024.bytes
            maxMemorySize = 1024.bytes
            minJournalCompactDelay = 5.seconds
          }.apply()
          val q = new PersistentQueue("aggressiverewritingemptyqueue", folderName, config, timer, scheduler)
          q.setup()

          // Leave 8 open transactions so that the journal rewrites
          // results in no shrinkage
          (1 to 8).foreach { _ =>
            q.add(new Array[Byte](1))
            q.remove
          }

          q.totalRewrites() mustEqual 1

          (1 to 8).foreach { _ =>
            q.add(new Array[Byte](1))
            q.remove
          }
          q.totalRewrites() mustEqual 2
        }
      }
    }


    "aggressive rewrites" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          val config = new QueueBuilder {
            defaultJournalSize = 160.bytes
            maxJournalSize = 256.bytes
            maxMemorySize = 256.bytes
            minJournalCompactDelay = 5.seconds
            disableAggressiveRewrites = false
          }.apply()
          val q = new PersistentQueue("aggresiverewriting", folderName, config, timer, scheduler)
          q.setup()

          // serialized QItem = 1 + 21 bytes, 8 are 176.bytes:
          (1 to 8).foreach { _ =>
            q.add(new Array[Byte](1))
            q.remove
          }

          q.totalRewrites() mustEqual 1

          (1 to 8).foreach { _ =>
            q.add(new Array[Byte](1))
            q.remove
          }

          q.totalRewrites() mustEqual 2

          time.advance(5.seconds)
          timer.timeout()
        }
      }
    }


    "recover the journal after a restart" in {
      withTempFolder {
        val q = new PersistentQueue("rolling", folderName, new QueueBuilder().apply(), timer, scheduler)
        q.setup
        q.add("first".getBytes)
        q.add("second".getBytes)
        new String(q.remove.get.data) mustEqual "first"
        q.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1
        q.close

        val q2 = new PersistentQueue("rolling", folderName, new QueueBuilder().apply(), timer, scheduler)
        q2.setup
        q2.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1
        new String(q2.remove.get.data) mustEqual "second"
        q2.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1 + 1
        q2.length mustEqual 0
        q2.close

        val q3 = new PersistentQueue("rolling", folderName, new QueueBuilder().apply(), timer, scheduler)
        q3.setup
        q3.journalSize mustEqual 5 + 6 + 16 + 16 + 5 + 5 + 1 + 1
        q3.length mustEqual 0
      }
    }

    "recover a journal with a rewritten transaction" in {
      withTempFolder {
        val q = new PersistentQueue("rolling", folderName, new QueueBuilder().apply(), timer, scheduler)
        q.setup()
        q.add("zero".getBytes)
        q.add("first".getBytes)
        q.add("second".getBytes)

        // force-bump xid
        val zero = q.remove(true).get
        new String(zero.data) mustEqual "zero"
        q.confirmRemove(zero.xid)

        val item = q.remove(true).get
        new String(item.data) mustEqual "first"
        q.forceRewrite(Failpoint.Default)
        q.confirmRemove(item.xid)
        q.close()

        val q2 = new PersistentQueue("rolling", folderName, new QueueBuilder().apply(), timer, scheduler)
        q2.setup()
        new String(q2.remove().get.data) mustEqual "second"
        q2.close()
        dumpJournal("rolling") mustEqual "add(5:0:first), remove-tentative(2), add(6:0:second), confirm-remove(2), remove"
      }
    }

    "recover a journal with a incomplete rewrite fail before pack" in {
      withTempFolder {
        interruptRewrites("incomplete-rewrite-1", Failpoint.RewriteFPBeforePack) mustEqual "add(4:0:zero), add(5:0:first), add(6:0:second), remove-tentative(1), confirm-remove(1), remove-tentative(2), unremove(2), remove, remove"
      }
    }

    "recover a journal with a incomplete rewrite fail after delete" in {
      withTempFolder {
        interruptRewrites("incomplete-rewrite-3", Failpoint.RewriteFPAfterDelete) mustEqual "add(5:0:first), remove-tentative(2), add(6:0:second), unremove(2), remove, remove"
      }
    }

    "rewrite empty queue on graceful shutdown" in {
      withTempFolder {
        val q = new PersistentQueue("gracefulshutdownempty", folderName, new QueueBuilder().apply(), timer, scheduler)
        q.setup()
        q.add("first".getBytes)
        q.add("second".getBytes)
        new String(q.remove().get.data) mustEqual "first"
        new String(q.remove().get.data) mustEqual "second"
        dumpJournal("gracefulshutdownempty") mustEqual "add(5:0:first), add(6:0:second), remove, remove"
        q.close(true)
        val q2 = new PersistentQueue("gracefulshutdownempty", folderName, new QueueBuilder().apply(), timer, scheduler)
        q2.setup()
        dumpJournal("gracefulshutdownempty") mustEqual ""
      }
    }

    "not write non-empty queue on graceful shutdown" in {
      withTempFolder {
        val q = new PersistentQueue("gracefulshutdownnonempty", folderName, new QueueBuilder().apply(), timer, scheduler)
        q.setup()
        q.add("first".getBytes)
        q.add("second".getBytes)
        new String(q.remove().get.data) mustEqual "first"
        dumpJournal("gracefulshutdownnonempty") mustEqual "add(5:0:first), add(6:0:second), remove"
        q.close(true)
        val q2 = new PersistentQueue("gracefulshutdownnonempty", folderName, new QueueBuilder().apply(), timer, scheduler)
        q2.setup()
        dumpJournal("gracefulshutdownnonempty") mustEqual "add(5:0:first), add(6:0:second), remove"
        new String(q2.remove().get.data) mustEqual "second"
      }
    }

    "rewrite empty queue with open transactions on graceful shutdown" in {
      withTempFolder {
        val q = new PersistentQueue("gracefulshutdownopentran", folderName, new QueueBuilder().apply(), timer, scheduler)
        q.setup()
        q.add("first".getBytes)
        q.add("second".getBytes)
        val first = q.remove(true).get
        new String(first.data) mustEqual "first"
        q.confirmRemove(first.xid)

        val second = q.remove(true).get
        new String(second.data) mustEqual "second"
        q.close(true)
        dumpJournal("gracefulshutdownopentran") mustEqual "add(6:0:second), remove-tentative(2)"
        val q2 = new PersistentQueue("gracefulshutdownopentran", folderName, new QueueBuilder().apply(), timer, scheduler)
        q2.setup()
        dumpJournal("gracefulshutdownopentran") mustEqual "add(6:0:second), remove-tentative(2), unremove(2)"
      }
    }

    "honor max_age" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          val config = new QueueBuilder {
            maxAge = 3.seconds
          }.apply()
          val q = new PersistentQueue("weather_updates", folderName, config, timer, scheduler)
          q.setup()
          q.add("sunny".getBytes) mustEqual true
          q.length mustEqual 1
          time.advance(4.seconds)
          q.remove mustEqual None

          q.config = new QueueBuilder {
            maxAge = 60.seconds
          }.apply()
          q.add("rainy".getBytes) mustEqual true
          q.config = new QueueBuilder {
            maxAge = 1.seconds
          }.apply()
          time.advance(5.seconds)
          q.remove mustEqual None
        }
      }
    }

    "allow max_journal_size and max_memory_size to be overridden per queue" in {
      withTempFolder {
        val config1 = new QueueBuilder {
          maxMemorySize = 123.bytes
        }.apply()
        val q1 = new PersistentQueue("test1", folderName, config1, timer, scheduler)
        q1.config.maxJournalSize mustEqual new QueueBuilder().apply().maxJournalSize
        q1.config.maxMemorySize mustEqual 123.bytes
        val config2 = new QueueBuilder {
          maxJournalSize = 123.bytes
        }.apply()
        val q2 = new PersistentQueue("test1", folderName, config2, timer, scheduler)
        q2.config.maxJournalSize mustEqual 123.bytes
        q2.config.maxMemorySize mustEqual new QueueBuilder().apply().maxMemorySize
      }
    }

    "handle timeout reads" in {
      "success" in {
        withTempFolder {
          val config1 = new QueueBuilder {
            maxMemorySize = 1.kilobyte
          }.apply()
          val q = new PersistentQueue("things", folderName, config1, timer, scheduler)
          q.setup

          var rv: Option[String] = None

          val deadline = 250.milliseconds.fromNow
          q.waitRemove(Some(deadline), false).onSuccess { item =>
            rv = item.map { x => new String(x.data) }
          }
          timer.deadline mustEqual deadline

          rv mustEqual None
          q.add("hello".getBytes)
          rv mustEqual Some("hello")
        }
      }

      "timeout" in {
        withTempFolder {
          val config1 = new QueueBuilder {
            maxMemorySize = 1.kilobyte
          }.apply()
          val q = new PersistentQueue("things", folderName, config1, timer, scheduler)
          q.setup

          var rv: Option[String] = Some("foo")

          val deadline = 250.milliseconds.fromNow
          q.waitRemove(Some(deadline), false).onSuccess { item =>
            rv = item.map { x => new String(x.data) }
          }
          timer.deadline mustEqual deadline

          rv mustEqual Some("foo")
          timer.timeout()
          rv mustEqual None
        }
      }

      "really long timeout is cancelled" in {
        val deadline = 7.days.fromNow

        "when an item arrives" in {
          withTempFolder {
            val q = new PersistentQueue("things", folderName, new QueueBuilder().apply(), timer, scheduler)
            q.setup()

            var rv: Option[String] = None
            q.waitRemove(Some(deadline), false).onSuccess { item =>
              rv = item.map { x => new String(x.data) }
            }
            q.waiterCount mustEqual 1
            timer.timerTask.cancelled mustEqual false

            q.add("hi".getBytes)

            rv mustEqual Some("hi")
            q.waiterCount mustEqual 0
            timer.timerTask.cancelled mustEqual true
          }
        }

        "when the connection dies" in {
          withTempFolder {
            val q = new PersistentQueue("things", folderName, new QueueBuilder().apply(), timer, scheduler)
            q.setup()

            var rv: Option[String] = None
            val future = q.waitRemove(Some(deadline), false)
            future.onSuccess { item =>
              rv = item.map { x => new String(x.data) }
            }
            q.waiterCount mustEqual 1
            timer.timerTask.cancelled mustEqual false

            future.cancel()
            q.waiterCount mustEqual 0
            timer.timerTask.cancelled mustEqual true
          }
        }
      }
    }

    "correctly interleave transactions in the journal" in {
      withTempFolder {
        val config = new QueueBuilder {
          maxMemorySize = 1.kilobyte
        }.apply()
        val q = new PersistentQueue("things", folderName, config, timer, scheduler)

        q.setup
        q.add("house".getBytes)
        q.add("cat".getBytes)
        dumpJournal("things") mustEqual "add(5:0:house), add(3:0:cat)"

        val house = q.remove(true).get
        new String(house.data) mustEqual "house"
        house.xid mustEqual 1
        dumpJournal("things") mustEqual "add(5:0:house), add(3:0:cat), remove-tentative(1)"

        val cat = q.remove(true).get
        new String(cat.data) mustEqual "cat"
        cat.xid mustEqual 2
        dumpJournal("things") mustEqual
          "add(5:0:house), add(3:0:cat), remove-tentative(1), remove-tentative(2)"

        q.unremove(house.xid)
        dumpJournal("things") mustEqual
          "add(5:0:house), add(3:0:cat), remove-tentative(1), remove-tentative(2), unremove(1)"

        q.confirmRemove(cat.xid)
        dumpJournal("things") mustEqual
          "add(5:0:house), add(3:0:cat), remove-tentative(1), remove-tentative(2), unremove(1), confirm-remove(2)"
        q.length mustEqual 1
        q.bytes mustEqual 5

        new String(q.remove.get.data) mustEqual "house"
        q.length mustEqual 0
        q.bytes mustEqual 0

        q.close
        dumpJournal("things") mustEqual
          "add(5:0:house), add(3:0:cat), remove-tentative(1), remove-tentative(2), unremove(1), confirm-remove(2), remove"

        // and journal is replayed correctly.
        val q2 = new PersistentQueue("things", folderName, config, timer, scheduler)
        q2.setup
        q2.length mustEqual 0
        q2.bytes mustEqual 0
      }
    }

    "recover a journal with open transactions" in {
      withTempFolder {
        val q = new PersistentQueue("things", folderName, new QueueBuilder().apply(), timer, scheduler)
        q.setup
        q.add("one".getBytes)
        q.add("two".getBytes)
        q.add("three".getBytes)
        q.add("four".getBytes)
        q.add("five".getBytes)

        val item1 = q.remove(true)
        item1 must beSome[QItem].which { item => new String(item.data) == "one" }
        new String(item1.get.data) mustEqual "one"
        val item2 = q.remove(true)
        new String(item2.get.data) mustEqual "two"
        val item3 = q.remove(true)
        new String(item3.get.data) mustEqual "three"
        val item4 = q.remove(true)
        new String(item4.get.data) mustEqual "four"

        q.confirmRemove(item2.get.xid)
        q.confirmRemove(item4.get.xid)
        q.close

        val q2 = new PersistentQueue("things", folderName, new QueueBuilder().apply(), timer, scheduler)
        q2.setup
        q2.length mustEqual 3
        q2.openTransactionCount mustEqual 0
        new String(q2.remove.get.data) mustEqual "one"
        new String(q2.remove.get.data) mustEqual "three"
        new String(q2.remove.get.data) mustEqual "five"
        q2.length mustEqual 0
      }
    }

    "continue a queue item" in {
      withTempFolder {
        val q = new PersistentQueue("things", folderName, new QueueBuilder().apply(), timer, scheduler)
        q.setup
        q.add("one".getBytes)

        val item1 = q.remove(true)
        new String(item1.get.data) mustEqual "one"

        q.continue(item1.get.xid, "two".getBytes)
        q.close

        val q2 = new PersistentQueue("things", folderName, new QueueBuilder().apply(), timer, scheduler)
        q2.setup
        q2.length mustEqual 1
        q2.openTransactionCount mustEqual 0
        new String(q2.remove.get.data) mustEqual "two"
        q2.length mustEqual 0
      }
    }

    "recreate the journal file when it gets too big" in {
      withTempFolder {
        val config = new QueueBuilder {
          maxJournalSize = 3.kilobytes
        }.apply()
        val q = new PersistentQueue("things", folderName, config, timer, scheduler)
        q.setup
        q.add(new Array[Byte](512))
        // can't roll the journal normally, cuz there's always one item left.
        for (i <- 0 until 4) {
          q.add(new Array[Byte](512))
          q.remove(false) must beSomeQItem(512)
        }
        q.add(new Array[Byte](512))
        q.length mustEqual 2
        q.journalSize mustEqual (512 * 6) + (6 * 21) + 4

        // next remove should force a recreate, because the queue size will be 512.
        q.remove(false) must beSomeQItem(512)
        q.length mustEqual 1
        q.journalSize mustEqual (512 + 21)

        // journal should contain exactly 1 item.
        q.close
        dumpJournal("things") mustEqual "add(512:0)"
      }
    }

    "don't recreate the journal file if the queue itself is still huge" in {
      withTempFolder {
        val config = new QueueBuilder {
          maxMemorySize = 1.kilobyte
          maxJournalSize = 3.kilobytes
        }.apply()
        val q = new PersistentQueue("things", folderName, config, timer, scheduler)
        q.setup
        for (i <- 0 until 8) {
          q.add(new Array[Byte](512))
        }
        q.length mustEqual 8
        q.bytes mustEqual 4096
        q.journalSize must be_<(q.journalTotalSize)
      }
    }

    "report an age of zero on an empty queue" in {
      withTempFolder {
        val q = new PersistentQueue("things", folderName, new QueueBuilder().apply(), timer, scheduler)
        q.setup
        put(q, 128, 0)
        Thread.sleep(10)
        q.remove() must beSomeQItem(128)
        q.length mustEqual 0
        q.currentAge mustEqual 0.milliseconds
      }
    }

    "report the age of the queue in the absence of gets" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          val q = new PersistentQueue("things", folderName, new QueueBuilder().apply(), timer, scheduler)
          q.setup
          put(q, 128, 0)
          put(q, 128, 0)
          q.remove()

          time.advance(10.milliseconds)
          q.currentAge mustEqual 10.milliseconds

          time.advance(10.milliseconds)
          q.currentAge mustEqual 20.milliseconds
        }
      }
    }

    "remove all stats" in {
      def stats(queueName: String): List[String] = {
        val prefix = "q/" + queueName + "/"
        List(Stats.getCounters(), Stats.getGauges(), Stats.getMetrics())
          .flatMap { _.map { case (k, _) => k } }
          .filter { _.startsWith(prefix) }
          .sorted
      }

      withTempFolder {
        Stats.clearAll()

        val statNamesBefore = stats("things")
        statNamesBefore must beEmpty

        val q = new PersistentQueue("things", folderName, new QueueBuilder().apply(), timer, scheduler)
        q.setup

        q.add(Array[Byte](1,2,3))
        q.add(Array[Byte](4,5,6))
        q.remove()

        q.removeStats()

        val statNamesAfter = stats("things")
        statNamesAfter must beEmpty
      }
    }

    "get oldest add time" in {
      withTempFolder {
        val q = new PersistentQueue("work", folderName, new QueueBuilder().apply(), timer, scheduler)
        q.setup
        q.length mustEqual 0

        q.add("item1".getBytes, None, None, Time.fromMilliseconds(2000))
        q.add("item2".getBytes, None, None, Time.fromMilliseconds(1000))
        q.add("item3".getBytes, None, None, Time.fromMilliseconds(3000))

        q.getOldestAddTime mustEqual 1000

        q.close
      }
    }
  }


  "PersistentQueue with no journal" should {
    val timer = new FakeTimer()
    val scheduler = new ScheduledThreadPoolExecutor(1)

    "create no journal" in {
      withTempFolder {
        val config = new QueueBuilder {
          keepJournal = false
        }.apply()
        val q = new PersistentQueue("mem", folderName, config, timer, scheduler)
        q.setup

        q.add("coffee".getBytes)
        new File(folderName, "mem").exists mustBe false
        q.remove must beSomeQItem("coffee")
      }
    }

    "lose all data after being destroyed" in {
      withTempFolder {
        val config = new QueueBuilder {
          keepJournal = false
        }.apply()
        val q = new PersistentQueue("mem", folderName, config, timer, scheduler)
        q.setup
        q.add("coffee".getBytes)
        q.close

        val q2 = new PersistentQueue("mem", folderName, config, timer, scheduler)
        q2.setup
        q2.remove mustEqual None
      }
    }
  }


  "PersistentQueue with item/size limit" should {
    val timer = new FakeTimer()
    val scheduler = new ScheduledThreadPoolExecutor(1)

    "honor max_items" in {
      withTempFolder {
        val config = new QueueBuilder {
          maxItems = 1
        }.apply()
        val q = new PersistentQueue("weather_updates", folderName, config, timer, scheduler)
        q.setup
        q.add("sunny".getBytes) mustEqual true
        q.add("rainy".getBytes) mustEqual false
        q.length mustEqual 1
        q.remove must beSomeQItem("sunny")
      }
    }

    "honor max_size" in {
      withTempFolder {
        val config = new QueueBuilder {
          maxSize = 510.bytes
        }.apply()
        val q = new PersistentQueue("weather_updates", folderName, config, timer, scheduler)
        q.setup
        q.add(("a" * 256).getBytes) mustEqual true
        q.add(("b" * 256).getBytes) mustEqual true
        q.add("television".getBytes) mustEqual false
        q.length mustEqual 2
        q.bytes mustEqual 512
        q.remove must beSomeQItem("a" * 256)
      }
    }

    "drop older items when discard_old_when_full is set" in {
      withTempFolder {
        val config = new QueueBuilder {
          maxItems = 3
          discardOldWhenFull = true
        }.apply()
        val q = new PersistentQueue("weather_updates", folderName, config, timer, scheduler)
        q.setup
        q.add("sunny".getBytes) mustEqual true
        q.add("rainy".getBytes) mustEqual true
        q.add("cloudy".getBytes) mustEqual true
        q.add("snowy".getBytes) mustEqual true
        q.length mustEqual 3
        q.remove must beSomeQItem("rainy")
      }
    }
  }

  "PersistentQueue with expiry" should {
    val timer = new FakeTimer()
    val scheduler = new ScheduledThreadPoolExecutor(1)

    "expire queue" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          val config = new QueueBuilder {
            keepJournal = false
            maxQueueAge = 90.seconds
          }.apply()
          val q = new PersistentQueue("wu_tang", folderName, config, timer, scheduler)
          q.setup()

          // Not ready, we just got started!
          q.isReadyForExpiration mustEqual false

          q.add("method man".getBytes, None) mustEqual true

          time.advance(30.seconds)
          // We aren't ready to expire yet, as it's not been long enough
          q.isReadyForExpiration mustEqual false

          time.advance(61.seconds)

          // Still not ready, as we have items in the queue!
          q.isReadyForExpiration mustEqual false

          q.remove must beSomeQItem("method man") // queue is now empty

          // This should be true now because the queue is 91 seconds old and
          // has no items
          q.isReadyForExpiration mustEqual true
        }
      }
    }
  }

  "PersistentQueue with item expiry" should {
    val timer = new FakeTimer()
    val scheduler = new ScheduledThreadPoolExecutor(1)

    "expire items into the ether" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          val config = new QueueBuilder {
            keepJournal = false
          }.apply()
          val q = new PersistentQueue("wu_tang", folderName, config, timer, scheduler)
          q.setup()
          val expiry = Time.now + 1.second
          q.add("rza".getBytes, Some(expiry)) mustEqual true
          q.add("gza".getBytes, Some(expiry)) mustEqual true
          q.add("ol dirty bastard".getBytes, Some(expiry)) mustEqual true
          q.add("raekwon".getBytes) mustEqual true
          time.advance(2.seconds)
          q.discardExpired() mustEqual 3
          q.length mustEqual 1
          q.remove must beSomeQItem("raekwon")
        }
      }
    }

    "expire items into a queue" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          val config = new QueueBuilder {
            keepJournal = false
          }.apply()
          val r = new PersistentQueue("rappers", folderName, config, timer, scheduler)
          val q = new PersistentQueue("wu_tang", folderName, config, timer, scheduler)
          r.setup()
          q.setup()
          q.expireQueue = Some(r)
          val expiry = Time.now + 1.second

          q.add("method man".getBytes, Some(expiry)) mustEqual true
          q.add("ghostface killah".getBytes, Some(expiry)) mustEqual true
          q.add("u-god".getBytes, Some(expiry)) mustEqual true
          q.add("masta killa".getBytes) mustEqual true
          time.advance(2.seconds)
          q.discardExpired() mustEqual 3
          q.length mustEqual 1
          q.remove must beSomeQItem("masta killa")

          r.length mustEqual 3
          r.remove must beSomeQItem("method man")
          r.remove must beSomeQItem("ghostface killah")
          r.remove must beSomeQItem("u-god")
        }
      }
    }

    "expire over maxExpireSweep number of items" in {
      withTempFolder {
        Time.withCurrentTimeFrozen { time =>
          val configWithMaxExpireSweep = new QueueBuilder {
            keepJournal = false
            maxExpireSweep = 3
          }.apply()
          val configWithoutMaxExpireSweep = new QueueBuilder {
            keepJournal = false
          }.apply()
          val r = new PersistentQueue("vocaloid", folderName, configWithoutMaxExpireSweep, timer, scheduler)
          val q = new PersistentQueue("wu_tang", folderName, configWithMaxExpireSweep, timer, scheduler)
          r.setup()
          q.setup()

          val expiryQ = Time.now + 1.second
          q.add("rza".getBytes, Some(expiryQ)) mustEqual true
          q.add("gza".getBytes, Some(expiryQ)) mustEqual true
          q.add("ol dirty bastard".getBytes, Some(expiryQ)) mustEqual true
          q.add("cappadonna".getBytes, Some(expiryQ)) mustEqual true
          q.add("raekwon".getBytes) mustEqual true
          time.advance(2.seconds)
          q.discardExpired() mustEqual 3
          q.length mustEqual 2
          q.remove must beSomeQItem("raekwon")
          q.length mustEqual 0

          val expiryR = Time.now + 1.second
          r.add("miku".getBytes, Some(expiryR)) mustEqual true
          r.add("rin".getBytes, Some(expiryR)) mustEqual true
          r.add("len".getBytes, Some(expiryR)) mustEqual true
          r.add("luka".getBytes, Some(expiryR)) mustEqual true
          r.add("geso".getBytes) mustEqual true
          time.advance(2.seconds)
          r.discardExpired() mustEqual 4
          r.length mustEqual 1
          r.remove must beSomeQItem("geso")
        }
      }
    }

    "whitelisted queues should disallow reads when clientId is not specified" in {
      withTempFolder {
        val configureWithWhitelist = new QueueBuilder {
          whiteListClientIdForDequeue = "test-client"
          maxExpireSweep = 3
        }.apply()

        val r = new PersistentQueue("white-list", folderName, configureWithWhitelist, timer, scheduler)
        r.setup()

        r.add("one".getBytes) mustEqual true
        r.add("two".getBytes) mustEqual true

        try {
          r.remove(true)
        } catch {
          case e:Exception => e.isInstanceOf[AvailabilityException] mustBe true
        }

        r.length mustEqual 2

        try {
          r.remove(false)
        } catch {
          case e:Exception => e.isInstanceOf[AvailabilityException] mustBe true
        }

        r.length mustEqual 2


      }
    }

    "whitelisted queues should disallow writes when clientId is not specified" in {
      withTempFolder {
        val configureWithWhitelist = new QueueBuilder {
          whiteListClientIdForEnqueue = "test-client"
          maxExpireSweep = 3
        }.apply()

        val r = new PersistentQueue("white-list-write", folderName, configureWithWhitelist, timer, scheduler)
        r.setup()

        try {
          r.add("one".getBytes)
        } catch {
          case e:Exception => e.isInstanceOf[AvailabilityException] mustBe true
        }

        r.length mustEqual 0

        try {
          r.remove()
        } catch {
          case _ => false mustBe true
        }
      }
    }
  }

  "PersistentQueue put future" should {
    val timer = new FakeTimer()
    val scheduler = new ScheduledThreadPoolExecutor(1)

    "timeout according to writeTimeout if write stalls" in {
      val config = new QueueBuilder {
        keepJournal = true
      }.apply()
      val q = new PersistentQueue("mem", new BlockingContainer(Duration.Top), config, timer, None)
      q.setup()
      val futureWrite = q.addDurable("kombucha".getBytes) 
      futureWrite.isDefined mustEqual true
      Await.result(futureWrite.get, 1.milliseconds) must throwA[com.twitter.util.TimeoutException]
    }

    "succeed after short blocking write" in {
      val config = new QueueBuilder {
        keepJournal = true
      }.apply()
      val q = new PersistentQueue("mem", new BlockingContainer(50.milliseconds), config, timer, None)
      q.setup()

      // future must not throw
      val futureWrite = q.addDurable("artisanal matcha".getBytes) 
      futureWrite.isDefined mustEqual true
      Await.result(futureWrite.get, 30.seconds) 
    }

    "succeed immediately for immediately completed future" in {
      val config = new QueueBuilder {
        keepJournal = true
      }.apply()
      val q = new PersistentQueue("mem", new BlockingContainer(Duration.Bottom), config, timer, None)
      q.setup()

      // future must not throw
      val futureWrite = q.addDurable("non fat double chocolate chip frappuccino".getBytes) 
      futureWrite.isDefined mustEqual true
      Await.result(futureWrite.get, Duration.Bottom) 
    }
  }
}
