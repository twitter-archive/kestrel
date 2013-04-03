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
import com.twitter.util.{Duration, FutureTask, Timer, TimerTask}
import java.io._
import java.net.InetSocketAddress
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

case object Quiescent extends Status(3) {
  val blocksReads = true
  val blocksWrites = true
  val gracefulShutdown = true
}

case object ReadOnly extends Status(2) {
  val blocksReads = false
  val blocksWrites = true
  val gracefulShutdown = true
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
      case _ => None
    }
  }
}

class ForbiddenStatusException extends Exception("Status forbidden.")
class UnknownStatusException extends Exception("Unknown status.")
class AlreadyStartedException extends Exception("Already started.")

class ServerStatus(val statusFile: String, val timer: Timer, val defaultStatus: Status = Quiescent,
                   val statusChangeGracePeriod: Duration = 30.seconds) {
  private val log = Logger.get(getClass.getName)

  private[this] var currentStatus: Status = defaultStatus
  @volatile private[this] var currentOperationStatus: Status = defaultStatus

  private[this] var timerTask: Option[TimerTask] = None

  private val statusStore = new File(statusFile)
  if (!statusStore.getParentFile.isDirectory) statusStore.getParentFile.mkdirs()

  private[this] val started = new AtomicBoolean(false)

  def start() {
    if (!started.compareAndSet(false, true)) {
      throw new AlreadyStartedException
    }

    loadStatus()

    Stats.addGauge("status/readable") {
      if (status.blocksReads) 0.0 else 1.0
    }

    Stats.addGauge("status/writeable") {
      if (status.blocksWrites) 0.0 else 1.0
    }
  }

  def addEndpoints(mainEndpoint: String, endpoints: Map[String, InetSocketAddress]) { }

  def shutdown() {
    synchronized {
      // Force completion of pending task
      timerTask.foreach { t =>
        t.cancel()
        log.debug("status change grace period canceled due shutdown")
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
  def setStatus(newStatus: Status): Status = {
    if (newStatus eq null) throw new UnknownStatusException
    if (newStatus == Down) throw new ForbiddenStatusException

    setStatus(newStatus, persistStatus = true, immediate = false)
  }

  private def setStatus(newStatus: Status, persistStatus: Boolean, immediate: Boolean): Status = synchronized {
    val oldStatus = currentStatus
    try {
      if (proposeStatusChange(oldStatus, newStatus)) {
        log.debug("Status change from %s to %s accepted", oldStatus, newStatus)
        currentStatus = newStatus
        statusChanged(oldStatus, newStatus, immediate)
        if (persistStatus) {
          storeStatus()
        }

        Stats.setLabel("status", currentStatus.toString)

        log.info("switched to status '%s' (previously '%s')", currentStatus, oldStatus)
      } else {
        log.warning("status change from '%s' to '%s' rejected", oldStatus, newStatus)
      }
    } catch { case e =>
      log.error(e, "unable to update server status from '%s' to '%s'", oldStatus, newStatus)
      timerTask.foreach { task =>
        task.cancel()
        log.warning("status change grace period canceled due to error")
      }
      timerTask = None
      currentStatus = oldStatus
      currentOperationStatus = oldStatus
      Stats.setLabel("status", currentStatus.toString)
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
          log.info("status change grace period expired; status '%s' now enforced", newStatus)
        }
        timerTask = Some(task)
      } else {
        currentOperationStatus = newStatus
      }
    } else {
      // less or same strictness
      timerTask.foreach { task =>
        task.cancel()
        log.debug("status change grace period canceled due to subsequent status change")
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
    if (statusStore.exists()) {
      val reader =
        new BufferedReader(new InputStreamReader(new FileInputStream(statusStore), "UTF-8"))
      val statusName =
        try {
          reader.readLine
        } catch { case e =>
          log.error(e, "unable to read stored status at '%s'; status remains '%s'", statusStore, currentStatus)
          defaultStatus.toString
        } finally {
          reader.close()
        }

      statusName match {
        case Status(newStatus) =>
          setStatus(newStatus, persistStatus = false, immediate = true)
        case _ =>
          log.error("unable to parse stored status '%s'; status remains '%s'", statusName, defaultStatus)
      }
    } else {
      log.info("no status stored at '%s'; status remains '%s'", statusStore, defaultStatus)
    }
  }

  private def storeStatus() {
    val writer = new OutputStreamWriter(new FileOutputStream(statusStore), "UTF-8")
    try {
      writer.write(status.toString + "\n")
      log.debug("stored status '%s' in '%s'", status, statusStore)
    } catch { case e =>
      log.error(e, "unable store status at '%s'", statusStore)
    } finally {
      writer.close()
    }
  }
}
