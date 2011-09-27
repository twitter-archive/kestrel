package net.lag.kestrel

import scala.annotation.tailrec

/**
 * Set of ids that maintains insert order.
 *
 * The "set" property is not enforced but is assumed because these are meant to be item ids.
 * Good performance assumes that operations are usually add, pop, popAll, or a remove of most
 * or all of the items. (Remove is O(n) so if there are many items and only a few are being
 * removed, performance will be bad.) This is tuned toward the general case where a client will
 * have either very few items open, or will have many items open but will remove them all at
 * once.
 */
class ItemIdList {
  private[this] var ids = new Array[Int](16)
  private[this] var head = 0
  private[this] var tail = 0

  def pop(count: Int): Seq[Int] = {
    if (count > tail - head) {
      Seq()
    } else {
      val rv = ids.slice(head, head + count)
      head += count
      rv
    }
  }

  def pop(): Option[Int] = pop(1).headOption

  def add(xid: Int) {
    if (head > 0) {
      compact()
    }
    if (tail == ids.size) {
      val bigger = new Array[Int](ids.size * 4)
      System.arraycopy(ids, 0, bigger, 0, ids.size)
      ids = bigger
    }
    ids(tail) = xid
    tail += 1
  }

  def add(xids: Seq[Int]) {
    xids.foreach(add)
  }

  def size: Int = tail - head

  def popAll(): Seq[Int] = {
    val rv = ids.slice(head, tail)
    head = 0
    tail = 0
    rv
  }

  def remove(xids: Set[Int]): Int = {
    var n = head
    var count = 0
    while (n < tail) {
      if (xids contains ids(n)) {
        ids(n) = 0
        count += 1
      }
      n += 1
    }
    compact()
    count
  }

  // for tests:
  def peek() = ids.slice(head, tail)

  def compact() { compact(0, head) }

  @tailrec
  private[this] def compact(start: Int, h1: Int) {
    if (h1 == tail) {
      // string of zeros. flatten tail and done.
      head = 0
      tail = start
    } else {
      // now find string of filled items.
      var h2 = h1 + 1
      while (h2 < tail && ids(h2) != 0) h2 += 1
      if (h1 != start) System.arraycopy(ids, h1, ids, start, h2 - h1)
      val newStart = start + h2 - h1
      while (h2 < tail && ids(h2) == 0) h2 += 1
      compact(newStart, h2)
    }
  }
}
