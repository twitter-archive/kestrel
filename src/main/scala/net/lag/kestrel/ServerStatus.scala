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
import com.twitter.logging.Logger
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Duration, Timer, TimerTask}
import java.io._
import java.util.concurrent.atomic.AtomicBoolean

abstract sealed class Status(val strictness: Int) {
  def stricterThan(that: Status) = (this.strictness - that.strictness) > 0
  def blocksReads: Boolean
  def blocksWrites: Boolean
  def gracefulShutdown: Boolean
}

case object Down extends Status(100) {
  val blocksReads = true
  val blocksWrites = true
  val gracefulShutdown = false
}

case object Quiescent extends Status(4) {
  val blocksReads = true
  val blocksWrites = true
  val gracefulShutdown = true
}

case object ReadOnly extends Status(3) {
  val blocksReads = false
  val blocksWrites = true
  val gracefulShutdown = true
}

case object WriteAvoid extends Status(2) {
  val blocksReads = false
  val blocksWrites = false
  val gracefulShutdown = false
}

case object Up extends Status(1) {
  val blocksReads = false
  val blocksWrites = false
  val gracefulShutdown = false
}

object Status {
  def unapply(name: String): Option[Status] = {
    Option(name).map(_.toUpperCase) match {
      case Some("DOWN") => Some(Down)
      case Some("QUIESCENT") => Some(Quiescent)
      case Some("READONLY") => Some(ReadOnly)
      case Some("UP") => Some(Up)
      case Some("WRITEAVOID") => Some(WriteAvoid)
      case _ => None
    }
  }
}

class ForbiddenStatusException extends Exception("Status forbidden.")
class UnknownStatusException extends Exception("Unknown status.")
class AlreadyStartedException extends Exception("Already started.")

class ServerStatus(val statusStore: PersistentMetadataStore, val timer: Timer, val defaultStatus: Status,
                   val statusChangeGracePeriod: Duration,
                   val statusName: Option[String]) {
  def this(statusFile: String, timer: Timer, defaultStatus: Status = Quiescent,
                     statusChangeGracePeriod: Duration = 30.seconds,
                     statusName: Option[String] = None) {
    this(new LocalMetadataStore(statusFile, statusName.map("status/%s".format(_)).getOrElse("status")),
      timer, defaultStatus, statusChangeGracePeriod, statusName)
  }

  private val log = Logger.get(getClass.getName)

  private[this] var currentStatus: Status = defaultStatus
  @volatile private[this] var currentOperationStatus: Status = defaultStatus

  private[this] var timerTask: Option[TimerTask] = None

  private[this] val started = new AtomicBoolean(false)

  protected def statusStatName(statName: String) =
    statusName.map("status/%s%s".format(_, statName)).getOrElse("status%s".format(statName))
  protected val statusLabel = statusStatName("")

  def start() {
    if (!started.compareAndSet(false, true)) {
      throw new AlreadyStartedException
    }

    loadStatus()

    Stats.addGauge(statusStatName("/readable")) {
      if (status.blocksReads) 0.0 else 1.0
    }

    Stats.addGauge(statusStatName("/writeable")) {
      if (status.blocksWrites) 0.0 else 1.0
    }
  }

  def shutdown() {
    synchronized {
      // Force completion of pending task
      timerTask.foreach { t =>
        t.cancel()
        log.debug("%s change grace period canceled due shutdown", statusLabel)
      }
      timerTask = None

      if (currentStatus != Down) {
        // do not persist this change
        setStatus(Down, persistStatus = false, immediate = true)
      }
    }
  }

  /**
   * Mark this host with an arbitrary status level. Returns the new status of the server,
   * which may be unchanged if a pending change is in progress.
   *
   * Note: cannot be used to mark this server as Down. Use Quiescent.
   */
  def setStatus(newStatus: Status, persistStatus: Boolean = true): Status = {
    if (newStatus eq null) throw new UnknownStatusException
    if (newStatus == Down) throw new ForbiddenStatusException

    setStatus(newStatus, persistStatus, immediate = false)
  }

  private def setStatus(newStatus: Status, persistStatus: Boolean, immediate: Boolean): Status = synchronized {
    val oldStatus = currentStatus
    try {
      if (proposeStatusChange(oldStatus, newStatus)) {
        log.debug("%s change from %s to %s accepted", statusLabel, oldStatus, newStatus)
        currentStatus = newStatus
        statusChanged(oldStatus, newStatus, immediate)
        if (persistStatus) {
          storeStatus()
        }

        Stats.setLabel(statusLabel, currentStatus.toString)

        log.info("%s switched to '%s' (previously '%s')", statusLabel, currentStatus, oldStatus)
      } else {
        log.warning("%s change from '%s' to '%s' rejected", statusLabel, oldStatus, newStatus)
      }
    } catch { case e =>
      log.error(e, "unable to update %s from '%s' to '%s'", statusLabel, oldStatus, newStatus)
      timerTask.foreach { task =>
        task.cancel()
        log.warning("%s change grace period canceled due to error", statusLabel)
      }
      timerTask = None
      currentStatus = oldStatus
      currentOperationStatus = oldStatus
      Stats.setLabel(statusLabel, currentStatus.toString)
      throw e
    }

    currentStatus
  }

  def status: Status = synchronized { currentStatus }

  /**
   * Invoked just before the status of the server changes.
   */
  protected def proposeStatusChange(oldStatus: Status, newStatus: Status): Boolean = {
    // ok to change if loosening restrictions OR no pending change
    (oldStatus stricterThan newStatus) || !timerTask.isDefined
  }

  /**
   * Invoked after the status of the server has changed.
   */
  protected def statusChanged(oldStatus: Status, newStatus: Status, immediate: Boolean) {
    if ((newStatus stricterThan oldStatus) && !immediate) {
      // e.g., Up -> ReadOnly (more strict)
      if (statusChangeGracePeriod > 0.seconds) {
        val task = timer.schedule(statusChangeGracePeriod.fromNow) {
          currentOperationStatus = newStatus
          timerTask = None
          log.info("%s change grace period expired; status '%s' now enforced", statusLabel, newStatus)
        }
        timerTask = Some(task)
      } else {
        currentOperationStatus = newStatus
      }
    } else {
      // less or same strictness
      timerTask.foreach { task =>
        task.cancel()
        log.debug("%s change grace period canceled due to subsequent status change", statusLabel)
      }
      timerTask = None
      currentOperationStatus = newStatus
    }
  }

  def blockReads = currentOperationStatus.blocksReads
  def blockWrites = currentOperationStatus.blocksWrites
  def gracefulShutdown = currentOperationStatus.gracefulShutdown

  /**
   * Mark this host as stopped.
   */
  def markQuiescent() {
    setStatus(Quiescent)
  }

  /**
   * Mark this host as avoiding writes (out of write serverset).
   */
  def markWriteAvoid() {
    setStatus(WriteAvoid)
  }

  /**
   * Mark this host read-only.
   */
  def markReadOnly() {
    setStatus(ReadOnly)
  }

  /**
   * Mark this host as up (available for reads & writes)
   */
  def markUp() {
    setStatus(Up)
  }

  /**
   * Mark this host with an arbitrary status level name. Names are case insensitive.
   *
   * Note: cannot be used to mark this server as Down. Use Quiescent.
   */
  def setStatus(statusName: String) {
    statusName match {
      case Status(newStatus) => setStatus(newStatus)
      case _ => throw new UnknownStatusException
    }
  }

  private def loadStatus() {
    if (statusStore.exists) {
      val statusName =
        try {
          statusStore.readMetadata
        } catch { case e =>
          log.error(e, "unable to read stored %s at '%s'; status remains '%s'", statusLabel, statusStore, currentStatus)
          defaultStatus.toString
        }

      statusName match {
        case Status(newStatus) =>
          setStatus(newStatus, persistStatus = false, immediate = true)
        case _ =>
          log.error("unable to parse stored %s '%s'; %s remains '%s'", statusLabel, statusName, statusLabel, defaultStatus)
      }
    } else {
      log.info("no %s stored at '%s'; %s remains '%s'", statusLabel, statusStore, statusLabel, defaultStatus)
    }
  }

  private def storeStatus() {
    statusStore.storeMetadata(status.toString)
  }
}
