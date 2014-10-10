/*
 * Copyright 2012 Twitter, Inc.
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

import org.specs.SpecificationWithJUnit

class ItemIdListSpec extends SpecificationWithJUnit {
  "ItemIdList" should {
    val iil = new ItemIdList()

    "add an Integer to the list" in {
      iil.add(3)
      iil.size mustEqual 1
    }

    "add a sequence of Integers to the list" in {
      iil.add(Seq(1, 2, 3, 4))
      iil.size mustEqual 4
    }

    "pop one item at a time" in {
      iil.add(Seq(90, 99))
      iil.pop() mustEqual Some(90)
      iil.pop() mustEqual Some(99)
    }

    "pop None when there's nothing to pop" in {
      iil.pop() mustEqual None
    }

    "pop all items from an index upward" in {
      iil.add(Seq(1, 2, 3, 4))
      val expected = Seq(1, 2)
      val actual = iil.pop(2)
      expected mustEqual actual
    }

    "pop all items from the list" in {
      val seq = Seq(12, 13, 14)
      iil.add(seq)
      iil.popAll() mustEqual seq
    }

    "return empty seq when pop's count is invalid" in {
      iil.pop(1) mustEqual Seq()
    }

    "remove a set of items from the list" in {
      iil.add(Seq(19, 7, 20, 22))
      val expected = Set(7, 20, 22)
      iil.remove(expected) mustEqual expected
    }

    "add and pop" in {
      iil.add(Seq(5, 4))
      iil.size mustEqual 2
      iil.pop() mustEqual Some(5)
      iil.pop() mustEqual Some(4)
      iil.pop() mustEqual None
    }

    "remove from the middle" in {
      iil.add(Seq(7, 6, 5, 4, 3, 2))
      iil.pop() mustEqual Some(7)
      iil.remove(Set(5, 4, 2)) mustEqual Set(5, 4, 2)
      iil.popAll() mustEqual Seq(6, 3)
    }

    "remove and pop combined" in {
      iil.add(Seq(7, 6, 5, 4, 3, 2))
      iil.remove(Set(6)) mustEqual Set(6)
      iil.pop() mustEqual Some(7)
      iil.pop() mustEqual Some(5)
      iil.popAll() mustEqual Seq(4, 3, 2)
    }
  }
}
