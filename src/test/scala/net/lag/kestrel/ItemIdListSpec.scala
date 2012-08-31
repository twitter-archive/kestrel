package net.lag.kestrel

import org.specs.Specification

class ItemIdListSpec extends Specification {
  "ItemIdList" should {
    var iil = new ItemIdList()

    doBefore {
      iil = new ItemIdList()
    }

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
      seq mustEqual iil.popAll()
    }

    "return empty seq when pop's count is invalid" in {
      iil.pop(1) mustEqual Seq()
    }

    "remove a set of items from the list" in {
      iil.add(Seq(19, 7, 20, 22))
      val expected = Set(7, 20, 22)
      expected mustEqual iil.remove(expected)
    }
  }
}
