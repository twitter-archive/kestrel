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

import _root_.org.specs.Specification


object JournalSpec extends Specification with TestHelper {
  "Journal" should {
    "walk" in {
      withTempFolder {
        val journal = new Journal(folderName + "/a1", false)
        journal.open()
        journal.add(QItem(0, 0, new Array[Byte](32), 0))
        journal.add(QItem(0, 0, new Array[Byte](64), 0))
        journal.add(QItem(0, 0, new Array[Byte](10), 0))
        journal.close()

        val journal2 = new Journal(folderName + "/a1", false)
        journal2.walk().map {
          case (item, itemsize) => item match {
            case JournalItem.Add(qitem) => qitem.data.size.toString
            case x => ""
          }
        }.mkString(",") mustEqual "32,64,10"
      }
    }
  }
}
