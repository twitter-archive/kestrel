/*
 * Copyright 2012 Twitter, Inc.
 * Copyright 2012 Robey Pointer <robeypointer@gmail.com>
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
import com.twitter.logging.TestLogging
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Duration, MockTimer, TempFolder, Time, TimeControl}
import java.io._
import org.specs.SpecificationWithJUnit
import org.specs.mock.{ClassMocker, JMocker}

class ServerStatusSpec extends SpecificationWithJUnit with JMocker with ClassMocker with TempFolder
with TestLogging {
  val mockTimer: MockTimer = new MockTimer

  def statusFile = canonicalFolderName + "/state"

  def makeServerStatus(
      proposeStatusChangeImpl: Option[(Status, Status) => Boolean] = None,
      statusChangedImpl: Option[(Status, Status, Boolean) => Unit] = None,
      statusChangeGracePeriod: Option[Duration] = None): ServerStatus = {

    val gracePeriod = statusChangeGracePeriod getOrElse { 30.seconds }
    val serverStatus = new ServerStatus(statusFile, mockTimer, statusChangeGracePeriod = gracePeriod) {
      override def proposeStatusChange(oldStatus: Status, newStatus: Status): Boolean = {
        super.proposeStatusChange(oldStatus, newStatus) &&
        proposeStatusChangeImpl.map { _(oldStatus, newStatus) }.getOrElse(true)
      }

      override def statusChanged(oldStatus: Status, newStatus: Status, immediate: Boolean) {
        super.statusChanged(oldStatus, newStatus, immediate)
        statusChangedImpl.foreach { _(oldStatus, newStatus, immediate) }
      }
    }
    serverStatus.start()
    serverStatus
  }

  def withServerStatus(f: (ServerStatus, TimeControl) => Unit) {
    withCustomizedServerStatus(None, None, None)(f)
  }

  def withCustomizedServerStatus(
      proposeStatusChangeImpl: Option[(Status, Status) => Boolean] = None,
      statusChangedImpl: Option[(Status, Status, Boolean) => Unit] = None,
      statusChangeGracePeriod: Option[Duration] = None)
      (f: (ServerStatus, TimeControl) => Unit) {
    withTempFolder {
      Time.withCurrentTimeFrozen { mutator =>
        val serverStatus = makeServerStatus(proposeStatusChangeImpl, statusChangedImpl, statusChangeGracePeriod)
        f(serverStatus, mutator)
      }
    }
  }

  def storedStatus(): String = {
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(statusFile), "UTF-8"))
    try {
      reader.readLine
    } finally {
      reader.close()
    }
  }

  def writeStatus(status: Status) {
    new File(statusFile).getParentFile.mkdirs()
    val writer = new OutputStreamWriter(new FileOutputStream(statusFile), "UTF-8")
    try {
      writer.write(status.toString)
    } finally {
      writer.close()
    }
  }

  "Status" should {
    "parse from strings to objects" in {
      Map("Up" -> Up,
          "Down" -> Down,
          "ReadOnly" -> ReadOnly,
          "Quiescent" -> Quiescent,
          "up" -> Up,
          "DOWN" -> Down,
          "readOnly" -> ReadOnly,
          "QuIeScEnT" -> Quiescent).foreach {
        case (name, expected) =>
          Status.unapply(name) mustEqual Some(expected)
      }
    }

    "handle unknown status" in {
      Status.unapply("wut?") mustEqual None
    }
  }

  "ServerStatus" should {
    "start only once" in {
      withServerStatus { (serverStatus, _) =>
        serverStatus.start() must throwA[Exception]
      }
    }

    "status" in {
      "default to quiescent" in {
        withServerStatus { (serverStatus, _) =>
          serverStatus.status mustEqual Quiescent
        }
      }

      "load previously stored status" in {
        withTempFolder {
          writeStatus(ReadOnly)
          val serverStatus = makeServerStatus()
          serverStatus.status mustEqual ReadOnly
        }
      }

      "switch to other statuses" in {
        withServerStatus { (serverStatus, tc) =>
          serverStatus.markUp()
          serverStatus.status mustEqual Up

          serverStatus.markReadOnly()
          serverStatus.status mustEqual ReadOnly

          serverStatus.markWriteAvoid()
          serverStatus.status mustEqual WriteAvoid

          tc.advance(31.seconds)
          mockTimer.tick()

          serverStatus.markQuiescent()
          serverStatus.status mustEqual Quiescent
        }
      }

      "switch to other statuses by values" in {
        withServerStatus { (serverStatus, tc) =>
          serverStatus.setStatus(Up)
          serverStatus.status mustEqual Up

          serverStatus.setStatus(ReadOnly)
          serverStatus.status mustEqual ReadOnly

          serverStatus.setStatus(WriteAvoid)
          serverStatus.status mustEqual WriteAvoid

          tc.advance(31.seconds)
          mockTimer.tick()

          serverStatus.setStatus(Quiescent)
          serverStatus.status mustEqual Quiescent
        }
      }

      "switch to other statuses by names" in {
        withServerStatus { (serverStatus, tc) =>
          serverStatus.setStatus("up")
          serverStatus.status mustEqual Up

          serverStatus.setStatus("readonly")
          serverStatus.status mustEqual ReadOnly

          serverStatus.setStatus("writeavoid")
          serverStatus.status mustEqual WriteAvoid

          tc.advance(31.seconds)
          mockTimer.tick()

          serverStatus.setStatus("quiescent")
          serverStatus.status mustEqual Quiescent
        }
      }

      "not switch to invalid statuses" in {
        withServerStatus { (serverStatus, _) =>
          serverStatus.setStatus(Down) must throwA[ForbiddenStatusException]
          serverStatus.setStatus("down") must throwA[ForbiddenStatusException]

          serverStatus.setStatus(null: Status) must throwA[UnknownStatusException]
          serverStatus.setStatus(null: String) must throwA[UnknownStatusException]

          serverStatus.setStatus("trolling") must throwA[UnknownStatusException]
        }
      }

      "switch to dead on shutdown" in {
        withServerStatus { (serverStatus, _) =>
          serverStatus.markUp()
          serverStatus.status mustEqual Up

          serverStatus.shutdown()
          serverStatus.status mustEqual Down
        }
      }

      "store status after successful change" in {
        withServerStatus { (serverStatus, _) =>
          new File(statusFile).exists() mustEqual false
          serverStatus.markUp()
          storedStatus() mustEqual "Up"
        }
      }

      "not store status after successful transient change" in {
        withServerStatus { (serverStatus, _) =>
          new File(statusFile).exists() mustEqual false
          serverStatus.markUp()
          serverStatus.setStatus(WriteAvoid, persistStatus = false)
          serverStatus.status == WriteAvoid
          storedStatus() mustEqual "Up"
        }
      }

      "not change status if previous change has not yet completed" in {
        withServerStatus { (serverStatus, tc) =>
          serverStatus.setStatus(Up) mustEqual Up
          serverStatus.setStatus(ReadOnly) mustEqual ReadOnly
          serverStatus.setStatus(Quiescent) mustEqual ReadOnly // rejected change

          tc.advance(31.seconds)
          mockTimer.tick()

          serverStatus.setStatus(Quiescent) mustEqual Quiescent
        }
      }

      "not change status if proposed change is rejected" in {
        var changes = 0
        val pscImpl = (oldStatus: Status, newStatus: Status) => {
          changes += 1
          changes <= 1
        }
        withCustomizedServerStatus(proposeStatusChangeImpl = Some(pscImpl)) { (serverStatus, tc) =>
          serverStatus.markQuiescent()
          storedStatus() mustEqual "Quiescent"

          tc.advance(31.seconds)
          mockTimer.tick()

          serverStatus.markUp()
          serverStatus.status mustEqual Quiescent
          storedStatus() mustEqual "Quiescent"
        }
      }

      "not store status after unsuccessful change" in {
        var changes = 0
        val scImpl = (oldStatus: Status, newStatus: Status, immediate: Boolean) => {
          changes += 1
          if (changes > 1) throw new RuntimeException("boom")
        }
        withCustomizedServerStatus(statusChangedImpl = Some(scImpl)) { (serverStatus, tc) =>
          serverStatus.markQuiescent()
          storedStatus() mustEqual "Quiescent"

          tc.advance(31.seconds)
          mockTimer.tick()

          serverStatus.markUp() must throwA[RuntimeException]
          storedStatus() mustEqual "Quiescent"
        }
      }

      "not store dead status on shutdown" in {
        withServerStatus { (serverStatus, _) =>
          serverStatus.markUp()
          storedStatus() mustEqual "Up"

          serverStatus.shutdown()
          serverStatus.status mustEqual Down
          storedStatus() mustEqual "Up"
        }
      }

      "update stats on status change" in {
        Stats.clearAll()
        withCustomizedServerStatus(statusChangeGracePeriod = Some(0.seconds)) { (serverStatus, _) =>
          serverStatus.setStatus(Up)
          serverStatus.status mustEqual Up
          Stats.getLabel("status") mustEqual Some("Up")
          Stats.getGauge("status/readable") mustEqual Some(1.0)
          Stats.getGauge("status/writeable") mustEqual Some(1.0)

          serverStatus.setStatus(WriteAvoid)
          serverStatus.status mustEqual WriteAvoid
          Stats.getLabel("status") mustEqual Some("WriteAvoid")
          Stats.getGauge("status/readable") mustEqual Some(1.0)
          Stats.getGauge("status/writeable") mustEqual Some(1.0)          

          serverStatus.setStatus(ReadOnly)
          serverStatus.status mustEqual ReadOnly
          Stats.getLabel("status") mustEqual Some("ReadOnly")
          Stats.getGauge("status/readable") mustEqual Some(1.0)
          Stats.getGauge("status/writeable") mustEqual Some(0.0)

          serverStatus.setStatus(Quiescent)
          serverStatus.status mustEqual Quiescent
          Stats.getLabel("status") mustEqual Some("Quiescent")
          Stats.getGauge("status/readable") mustEqual Some(0.0)
          Stats.getGauge("status/writeable") mustEqual Some(0.0)
        }
      }
    }

    "blocked operations" in {
      "do not block write operations when switching to WriteAvoid" in {
        withServerStatus { (serverStatus, tc) =>
          serverStatus.markUp()
          serverStatus.blockWrites mustEqual false
          serverStatus.blockReads mustEqual false
          serverStatus.setStatus(WriteAvoid) mustEqual WriteAvoid

          serverStatus.blockWrites mustEqual false
          serverStatus.blockReads mustEqual false

          tc.advance(31.seconds)
          mockTimer.tick()

          serverStatus.blockWrites mustEqual false
          serverStatus.blockReads mustEqual false
        }
      }

      "block write operations when switching to ReadOnly, after a delay" in {
        withServerStatus { (serverStatus, tc) =>
          serverStatus.markUp()
          serverStatus.blockWrites mustEqual false
          serverStatus.blockReads mustEqual false
          serverStatus.setStatus(ReadOnly) mustEqual ReadOnly

          serverStatus.blockWrites mustEqual false
          serverStatus.blockReads mustEqual false

          tc.advance(31.seconds)
          mockTimer.tick()

          serverStatus.blockWrites mustEqual true
          serverStatus.blockReads mustEqual false
        }
      }

      "block read/write operations when switching to Quiescent, with configured delay" in {
        withServerStatus { (serverStatus, tc) =>
          serverStatus.markUp()
          serverStatus.blockWrites mustEqual false
          serverStatus.blockReads mustEqual false
          serverStatus.setStatus(Quiescent) mustEqual Quiescent

          serverStatus.blockWrites mustEqual false
          serverStatus.blockReads mustEqual false

          tc.advance(31.seconds)
          mockTimer.tick()

          serverStatus.blockWrites mustEqual true
          serverStatus.blockReads mustEqual true
        }
      }

      "block read/write operations when switching to Quiescent, without delay" in {
        withCustomizedServerStatus(statusChangeGracePeriod = Some(0.seconds)) { (serverStatus, tc) =>
          serverStatus.markUp()
          serverStatus.blockWrites mustEqual false
          serverStatus.blockReads mustEqual false

          serverStatus.setStatus(Quiescent) mustEqual Quiescent
          mockTimer.tasks.isEmpty mustEqual true
          serverStatus.blockWrites mustEqual true
          serverStatus.blockReads mustEqual true
        }
      }

      "revert blocked operations immediately after an unsuccessful change" in {
        var changes = 0
        val scImpl = (oldStatus: Status, newStatus: Status, immediate: Boolean) => {
          changes += 1
          if (changes > 1) throw new RuntimeException("boom")
        }
        withCustomizedServerStatus(statusChangedImpl = Some(scImpl)) { (serverStatus, tc) =>
          serverStatus.markReadOnly()
          tc.advance(31.seconds)
          mockTimer.tick()

          serverStatus.blockWrites mustEqual true
          serverStatus.blockReads mustEqual false

          serverStatus.setStatus(Quiescent) must throwA[RuntimeException]

          serverStatus.blockWrites mustEqual true
          serverStatus.blockReads mustEqual false
        }
      }

      "immediately allow additional operations" in {
        withServerStatus { (serverStatus, tc) =>
          serverStatus.markReadOnly()
          tc.advance(31.seconds)
          mockTimer.tick()

          serverStatus.blockWrites mustEqual true
          serverStatus.blockReads mustEqual false

          serverStatus.setStatus(Up) mustEqual Up
          serverStatus.blockWrites mustEqual false
          serverStatus.blockReads mustEqual false
        }
      }
    }
  }
}
