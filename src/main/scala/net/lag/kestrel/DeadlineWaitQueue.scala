/*
 * Copyright 2011 Twitter, Inc.
 * Copyright 2011 Robey Pointer <robeypointer@gmail.com>
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

import java.util.LinkedHashSet
import scala.collection.JavaConversions
import com.twitter.util.{Time, Timer, TimerTask}

/**
 * A wait queue where each item has a timeout.
 * On each `trigger()`, one waiter is awoken (the awaken function is called). If the timeout is
 * triggered by the Timer, the timeout function will be called instead. The queue promises that
 * exactly one of the functions will be called, never both.
 */
final class DeadlineWaitQueue(timer: Timer) {
  case class Waiter(var timerTask: TimerTask, awaken: () => Unit)
  private val queue = JavaConversions.asScalaSet(new LinkedHashSet[Waiter])

  def add(deadline: Time, awaken: () => Unit, onTimeout: () => Unit) = {
    val waiter = Waiter(null, awaken)
    val timerTask = timer.schedule(deadline) {
      if (synchronized { queue.remove(waiter) }) onTimeout()
    }
    waiter.timerTask = timerTask
    synchronized { queue.add(waiter) }
    waiter
  }

  def remove(waiter: Waiter) {
    synchronized { queue.remove(waiter) }
    waiter.timerTask.cancel()
  }

  def trigger() {
    synchronized {
      queue.headOption.map { waiter =>
        queue.remove(waiter)
        waiter
      }
    }.foreach { waiter =>
      waiter.timerTask.cancel()
      waiter.awaken()
    }
  }

  def triggerAll() {
    synchronized {
      val rv = queue.toArray
      queue.clear()
      rv
    }.foreach { waiter =>
      waiter.timerTask.cancel()
      waiter.awaken()
    }
  }

  def size() = {
    synchronized { queue.size }
  }
}
