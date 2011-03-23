package net.lag.kestrel

import java.io.File
import scala.collection.mutable
import com.twitter.util.TempFolder

trait DumpJournal { self: TempFolder =>
  def dumpJournal(qname: String): String = {
    var rv = new mutable.ListBuffer[JournalItem]
    new Journal(new File(folderName, qname).getCanonicalPath).replay(qname) { item => rv += item }
    rv map {
      case JournalItem.Add(item) =>
        if (item.data.size > 0 && item.data(0) > 0) {
          "add(%d:%d:%s)".format(item.data.size, item.xid, new String(item.data))
        } else {
          "add(%d:%d)".format(item.data.size, item.xid)
        }
      case JournalItem.Remove => "remove"
      case JournalItem.RemoveTentative => "remove-tentative"
      case JournalItem.SavedXid(xid) => "xid(%d)".format(xid)
      case JournalItem.Unremove(xid) => "unremove(%d)".format(xid)
      case JournalItem.ConfirmRemove(xid) => "confirm-remove(%d)".format(xid)
    } mkString ", "
  }
}
