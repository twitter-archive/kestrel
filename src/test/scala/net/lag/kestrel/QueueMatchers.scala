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

import com.twitter.libkestrel.QueueItem
import org.specs.matcher.Matcher

trait QueueMatchers {
  def beSomeQueueItem(s: String) = new Matcher[Option[QueueItem]] {
    def apply(QueueItemEval: => Option[QueueItem]) = {
      val QueueItem = QueueItemEval
      (QueueItem.isDefined && (new String(QueueItem.get.data) == s), "ok", "wrong or missing queue item")
    }
  }

  def beSomeQueueItem(len: Int) = new Matcher[Option[QueueItem]] {
    def apply(QueueItemEval: => Option[QueueItem]) = {
      val QueueItem = QueueItemEval
      (QueueItem.isDefined && (QueueItem.get.data.size == len), "ok", "wrong or missing queue item")
    }
  }

  def beSomeQueueItem(len: Int, n: Int) = new Matcher[Option[QueueItem]] {
    def apply(QueueItemEval: => Option[QueueItem]) = {
      val QueueItem = QueueItemEval
      (QueueItem.isDefined && (QueueItem.get.data.size == len) && (QueueItem.get.data(0) == n),
        "ok", "wrong or missing queue item at " + n + "; got " + QueueItem.get.data(0))
    }
  }
}
