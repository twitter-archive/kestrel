package net.lag.kestrel

import java.io.File
import scala.collection.mutable
import com.twitter.util.TempFolder
import com.twitter.util.Time

trait DumpJournal { self: TempFolder =>
  def dumpJournal(qname: String, dumpTimestamps: Boolean = false): String = {
    var rv = new mutable.ListBuffer[JournalItem]
    new Journal(new File(folderName, qname).getCanonicalPath).replay { item => rv += item }
    rv map {
      case JournalItem.Add(item) =>
        if (!dumpTimestamps) {
          if (item.data.size > 0 && item.data(0) > 0) {
            "add(%d:%d:%s)".format(item.data.size, item.xid, new String(item.data))
          } else {
            "add(%d:%d)".format(item.data.size, item.xid)
          }
        }
        else {
          if (item.data.size > 0 && item.data(0) > 0) {
            "add(%d:%d:%s:%d:%d)".format(item.data.size, item.xid, new String(item.data), item.addTime.inMilliseconds, item.expiry.getOrElse(Time.fromMilliseconds(0)).inMilliseconds)
          } else {
            "add(%d:%d:%d:%d)".format(item.data.size, item.xid, item.addTime.inMilliseconds, item.expiry.getOrElse(Time.fromMilliseconds(0)).inMilliseconds)
          }
        }
      case JournalItem.Remove => "remove"
      case JournalItem.RemoveTentative(xid) => "remove-tentative(%d)".format(xid)
      case JournalItem.SavedXid(xid) => "xid(%d)".format(xid)
      case JournalItem.Unremove(xid) => "unremove(%d)".format(xid)
      case JournalItem.ConfirmRemove(xid) => "confirm-remove(%d)".format(xid)
    } mkString ", "
  }
}
