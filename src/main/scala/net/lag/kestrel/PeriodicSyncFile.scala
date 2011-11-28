package net.lag.kestrel

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentLinkedQueue, ScheduledExecutorService, ScheduledFuture, TimeUnit}
import com.twitter.conversions.time._
import com.twitter.util._
import java.io.{IOException, FileOutputStream, File}

abstract class PeriodicSyncTask(val scheduler: ScheduledExecutorService, initialDelay: Duration, period: Duration)
extends Runnable {
  @volatile private[this] var scheduledFsync: Option[ScheduledFuture[_]] = None

  def start: Unit = synchronized {
    if (scheduledFsync.isEmpty && period > 0.seconds) {
      val handle = scheduler.scheduleWithFixedDelay(this, initialDelay.inMilliseconds, period.inMilliseconds,
                                                    TimeUnit.MILLISECONDS)
      scheduledFsync = Some(handle)
    }
  }

  def stop: Unit = synchronized { _stop }

  def stopIf(f: => Boolean): Unit = synchronized {
    if (f) _stop
  }

  private[this] def _stop = {
    scheduledFsync.foreach { _.cancel(false) }
    scheduledFsync = None
  }
}

/**
 * Open a file for writing, and fsync it on a schedule. The period may be 0 to force an fsync
 * after every write, or `Duration.MaxValue` to never fsync.
 */
class PeriodicSyncFile(file: File, scheduler: ScheduledExecutorService, period: Duration) {
  // pre-completed future for writers who are behaving synchronously.
  private final val DONE = Future(())

  val writer = new FileOutputStream(file, true).getChannel
  val promises = new ConcurrentLinkedQueue[Promise[Unit]]()
  val periodicSyncTask = new PeriodicSyncTask(scheduler, period, period) {
    override def run() {
      if (!closed && !promises.isEmpty) fsync()
    }
  }

  @volatile var closed = false

  private def fsync() {
    synchronized {
      // race: we could underestimate the number of completed writes. that's okay.
      val completed = promises.size
      try {
        writer.force(false)
      } catch {
        case e: IOException =>
          for (i <- 0 until completed) {
            promises.poll().setException(e)
          }
        return;
      }

      for (i <- 0 until completed) {
        promises.poll().setValue(())
      }

      periodicSyncTask.stopIf { promises.isEmpty }
    }
  }

  def write(buffer: ByteBuffer): Future[Unit] = {
    do {
      writer.write(buffer)
    } while (buffer.position < buffer.limit)
    if (period == 0.seconds) {
      try {
        writer.force(false)
        DONE
      } catch {
        case e: IOException =>
          Future.exception(e)
      }
    } else if (period == Duration.MaxValue) {
      // not fsync'ing.
      DONE
    } else {
      val promise = new Promise[Unit]()
      promises.add(promise)
      periodicSyncTask.start
      promise
    }
  }

  /**
   * No locking is done here. Be sure you aren't doing concurrent writes or they may be lost
   * forever, and nobody will cry.
   */
  def close() {
    closed = true
    periodicSyncTask.stop
    fsync()
    writer.close()
  }

  def position: Long = writer.position()
}
