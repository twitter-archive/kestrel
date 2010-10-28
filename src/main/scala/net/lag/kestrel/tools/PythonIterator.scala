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

package net.lag.kestrel.tools

abstract class PythonIterator[T] extends (() => Option[T]) with Iterator[T] {
  var nextItem: Option[T] = None
  var alive = true

  def hasNext = {
    if (alive && !nextItem.isDefined) {
      nextItem = apply()
      if (!nextItem.isDefined) alive = false
    }
    alive
  }

  def next() = {
    val rv = nextItem.get
    nextItem = None
    rv
  }
}
