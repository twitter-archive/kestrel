package com.twitter.scarling

import java.io.{File, FileInputStream}
import scala.util.Sorting
import sorg.testing._


object QueueCollectionTests extends Tests {
    
    override def testName = "PersistentQueueTests"
    
    private def sorted[T <% Ordered[T]](list: List[T]): List[T] = {
        val dest = list.toArray
        Sorting.quickSort(dest)
        dest.toList
    }
    
    override def setUp = {
        qc = new QueueCollection(currentFolder.getPath)
    }
    
    override def tearDown = {
        qc.shutdown
    }
    
    private var qc: QueueCollection = null
    
    
    test("queue creation") {
        expect(List()) { qc.queueNames }

        qc.add("work1", "stuff".getBytes)
        qc.add("work2", "other stuff".getBytes)

        expect(List("work1", "work2")) { sorted(qc.queueNames) }
        expect(16) { qc.currentBytes }
        expect(2) { qc.currentItems }
        expect(2) { qc.totalAdded }

        expect("stuff") { new String(qc.remove("work1").get) }
        expect(None) { qc.remove("work1") }
        expect("other stuff") { new String(qc.remove("work2").get) }
        expect(None) { qc.remove("work2") }

        expect(0) { qc.currentBytes }
        expect(0) { qc.currentItems }
        expect(2) { qc.totalAdded }
    }
    
    test("load from journal") {
        qc.add("ducklings", "huey".getBytes)
        qc.add("ducklings", "dewey".getBytes)
        qc.add("ducklings", "louie".getBytes)
        expect(List("ducklings")) { qc.queueNames }
        expect(14) { qc.currentBytes }
        expect(3) { qc.currentItems }
        qc.shutdown
        
        qc = new QueueCollection(currentFolder.getPath)
        expect(List()) { qc.queueNames }
        expect("huey") { new String(qc.remove("ducklings").get) }
        // now the queue should be suddenly instantiated:
        expect(10) { qc.currentBytes }
        expect(2) { qc.currentItems }
    }
    
    test("queue hit/miss tracking") {
        qc.add("ducklings", "ugly1".getBytes)
        qc.add("ducklings", "ugly2".getBytes)
        expect(0) { qc.queueHits }
        expect(0) { qc.queueMisses }
        
        expect("ugly1") { new String(qc.remove("ducklings").get) }
        expect(1) { qc.queueHits }
        expect(0) { qc.queueMisses }
        expect(None) { qc.remove("zombie") }
        expect(1) { qc.queueHits }
        expect(1) { qc.queueMisses }
        
        expect("ugly2") { new String(qc.remove("ducklings").get) }
        expect(2) { qc.queueHits }
        expect(1) { qc.queueMisses }
        expect(None) { qc.remove("ducklings") }
        expect(2) { qc.queueHits }
        expect(2) { qc.queueMisses }
        expect(None) { qc.remove("ducklings") }
        expect(2) { qc.queueHits }
        expect(3) { qc.queueMisses }
    }
}
