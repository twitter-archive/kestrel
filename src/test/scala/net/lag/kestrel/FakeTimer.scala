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

import com.twitter.util.{Duration, Time, Timer, TimerTask}

class FakeTimer extends Timer {
  val timerTask = new TimerTask {
    var cancelled = false

    def cancel() { cancelled = true }
  }

  var deadline: Time = Time.epoch
  var repeat: Option[Duration] = None
  var timeout: () => Unit = { () => }

  def schedule(when: Time)(f: => Unit): TimerTask = {
    deadline = when
    repeat = None
    timeout = { () => f }
    timerTask
  }

  def schedule(when: Time, period: Duration)(f: => Unit): TimerTask = {
    deadline = when
    repeat = Some(period)
    timeout = { () => f }
    timerTask
  }

  def stop() { }
}
