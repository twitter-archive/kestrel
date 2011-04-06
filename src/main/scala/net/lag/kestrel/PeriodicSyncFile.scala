package net.lag.kestrel

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import com.twitter.conversions.time._
import com.twitter.util._
import java.io.{IOException, FileOutputStream, File}

/**
 * Open a file for writing, and fsync it on a schedule. The period may be 0 to force an fsync
 * after every write, or `Duration.MaxValue` to never fsync.
 */
class PeriodicSyncFile(file: File, timer: Timer, period: Duration) {
  val writer = new FileOutputStream(file, true).getChannel
  val promises = new ConcurrentLinkedQueue[Promise[Unit]]()

  @volatile var closed = false

  if (period > 0.seconds && period < Duration.MaxValue) {
    timer.schedule(Time.now, period) {
      if (!closed) fsync()
    }
  }

  private def fsync() {
    // race: we could underestimate the number of completed writes. that's okay.
    val completed = promises.size
    try {
      writer.force(false)
      for (i <- 0 until completed) {
        promises.poll().setValue(())
      }
    } catch {
      case e: IOException =>
        for (i <- 0 until completed) {
          promises.poll().setException(e)
        }
    }
  }

  def write(buffer: ByteBuffer): Future[Unit] = {
    val promise = new Promise[Unit]()
    do {
      writer.write(buffer)
    } while (buffer.position < buffer.limit)
    if (period == 0.seconds) {
      try {
        writer.force(false)
        promise.setValue(())
      } catch {
        case e: IOException =>
          promise.setException(e)
      }
    } else if (period == Duration.MaxValue) {
      // not fsync'ing.
      promise.setValue(())
    } else {
      promises.add(promise)
    }
    promise
  }

  /**
   * No locking is done here. Be sure you aren't doing concurrent writes or they may be lost
   * forever, and nobody will cry.
   */
  def close() {
    closed = true
    fsync()
    writer.close()
  }

  def position: Long = writer.position()
}
