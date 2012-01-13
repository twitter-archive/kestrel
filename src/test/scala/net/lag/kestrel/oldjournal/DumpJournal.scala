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
package oldjournal

import com.twitter.util.TempFolder
import java.io.File
import scala.collection.mutable

trait DumpJournal { self: TempFolder =>
  def dumpJournal(qname: String): String = {
    var rv = new mutable.ListBuffer[JournalItem]
    new Journal(new File(folderName, qname).getCanonicalPath).replay { item => rv += item }
    rv map {
      case JournalItem.Add(item) =>
        if (item.data.size > 0 && item.data(0) > 0) {
          "add(%d:%d:%s)".format(item.data.size, item.xid, new String(item.data))
        } else {
          "add(%d:%d)".format(item.data.size, item.xid)
        }
      case JournalItem.Remove => "remove"
      case JournalItem.RemoveTentative(xid) => "remove-tentative(%d)".format(xid)
      case JournalItem.SavedXid(xid) => "xid(%d)".format(xid)
      case JournalItem.Unremove(xid) => "unremove(%d)".format(xid)
      case JournalItem.ConfirmRemove(xid) => "confirm-remove(%d)".format(xid)
    } mkString ", "
  }
}
