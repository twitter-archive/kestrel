package com.twitter.scarling

import java.io.{File, FileInputStream}
import sorg.testing._


object PersistentQueueTests extends Tests {
    
    override def testName = "PersistentQueueTests"
    
    private val tempFolder = new File("./temp-test").getAbsoluteFile
    
    override def setUp = {
        tempFolder.mkdir
    }
    
    override def tearDown = {
        for (f <- tempFolder.listFiles) f.delete
        tempFolder.delete
    }

    
    test("add and remove one item") {
        val q = new PersistentQueue(tempFolder.getPath, "work")

        expect(0) { q.size }
        expect(0) { q.totalItems }
        expect(0) { q.bytes }
        expect(0) { q.journalSize }
        
        q.add("hello kitty".getBytes)
        
        expect(1) { q.size }
        expect(1) { q.totalItems }
        expect(11) { q.bytes }
        expect(16) { q.journalSize }

        expect("hello kitty") { new String(q.remove) }
        
        expect(0) { q.size }
        expect(1) { q.totalItems }
        expect(0) { q.bytes }
        expect(17) { q.journalSize }

        q.close
        
        val f = new FileInputStream(new File(tempFolder, "work"))
        val data = new Array[Byte](17)
        f.read(data)
        expect("0:0:0:0:11:104:101:108:108:111:32:107:105:116:116:121:1") { data.mkString(":") }
    }
    
    test("journal rotation") {
        val q = new PersistentQueue(tempFolder.getPath, "rolling")
        q.maxJournalSize = 64
        
        q.add(new Array[Byte](32))
        q.add(new Array[Byte](64))
        expect(2) { q.size }
        expect(2) { q.totalItems }
        expect(96) { q.bytes }
        expect(106) { q.journalSize }
        expect(106) { new File(tempFolder.getPath, "rolling").length }
        
        q.remove
        expect(1) { q.size }
        expect(2) { q.totalItems }
        expect(64) { q.bytes }
        expect(107) { q.journalSize }
        expect(107) { new File(tempFolder.getPath, "rolling").length }
        
        // now it should rotate:
        q.remove
        expect(0) { q.size }
        expect(2) { q.totalItems }
        expect(0) { q.bytes }
        expect(0) { q.journalSize }
        expect(0) { new File(tempFolder.getPath, "rolling").length }
    }
}
