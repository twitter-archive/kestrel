/** Copyright 2008 Twitter, Inc. */
package com.twitter.scarling

import scala.util.Sorting
import java.io.{File, FileInputStream}
import net.lag.configgy.Config
import org.specs._


object QueueCollectionSpec extends Specification with TestHelper {

  private var qc: QueueCollection = null

  private def sorted[T <% Ordered[T]](list: List[T]): List[T] = {
    val dest = list.toArray
    Sorting.quickSort(dest)
    dest.toList
  }

  "QueueCollection" should {
    doAfter {
      qc.shutdown
    }


    "create a queue" in {
      withTempFolder {
        qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
        qc.queueNames mustEqual Nil

        qc.add("work1", "stuff".getBytes)
        qc.add("work2", "other stuff".getBytes)

        sorted(qc.queueNames) mustEqual List("work1", "work2")
        qc.currentBytes mustEqual 16
        qc.currentItems mustEqual 2
        qc.totalAdded mustEqual 2

        new String(qc.remove("work1").get) mustEqual "stuff"
        qc.remove("work1") mustEqual None
        new String(qc.remove("work2").get) mustEqual "other stuff"
        qc.remove("work2") mustEqual None

        qc.currentBytes mustEqual 0
        qc.currentItems mustEqual 0
        qc.totalAdded mustEqual 2
      }
    }

    "load from journal" in {
      withTempFolder {
        qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
        qc.add("ducklings", "huey".getBytes)
        qc.add("ducklings", "dewey".getBytes)
        qc.add("ducklings", "louie".getBytes)
        qc.queueNames mustEqual List("ducklings")
        qc.currentBytes mustEqual 14
        qc.currentItems mustEqual 3
        qc.shutdown

        qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
        qc.queueNames mustEqual Nil
        new String(qc.remove("ducklings").get) mustEqual "huey"
        // now the queue should be suddenly instantiated:
        qc.currentBytes mustEqual 22
        qc.currentItems mustEqual 2
      }
    }

    "queue hit/miss tracking" in {
      withTempFolder {
        qc = new QueueCollection(folderName, Config.fromMap(Map.empty))
        qc.add("ducklings", "ugly1".getBytes)
        qc.add("ducklings", "ugly2".getBytes)
        qc.queueHits mustEqual 0
        qc.queueMisses mustEqual 0

        new String(qc.remove("ducklings").get) mustEqual "ugly1"
        qc.queueHits mustEqual 1
        qc.queueMisses mustEqual 0
        qc.remove("zombie") mustEqual None
        qc.queueHits mustEqual 1
        qc.queueMisses mustEqual 1

        new String(qc.remove("ducklings").get) mustEqual "ugly2"
        qc.queueHits mustEqual 2
        qc.queueMisses mustEqual 1
        qc.remove("ducklings") mustEqual None
        qc.queueHits mustEqual 2
        qc.queueMisses mustEqual 2
        qc.remove("ducklings") mustEqual None
        qc.queueHits mustEqual 2
        qc.queueMisses mustEqual 3
      }
    }
  }
}
