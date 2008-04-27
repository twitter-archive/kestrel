package sorg.testing;

import java.io.File
import scala.testing.SUnit._


abstract class Tests extends Test with Assert {
    type TestExp = () => Unit;
    var tests = List[Pair[String, TestExp]]();

    def testName = "tests"
    
    def test(desc: String)(t: => Unit) : Unit = {
        // careful: this pushes the tests in reverse order like a stack, so
        // we have to reverse them back later.
        tests = Pair(desc, () => runUnitTest(t)) :: tests
    }

    def setUp = { }
    def tearDown = { }
    
    override def run(tr: TestResult) = {
        for (val Pair(desc, expression) <- tests.reverse) new TestCase(desc) {
            override def runTest() = {
                Console.print("    " + testName + ":" + desc + " ...")
                Console.flush
                try {
                    expression()
                    Console.println(" OK")
                } catch {
                    case x => {
                        Console.println(" FAIL")
                        throw x
                    }
                }
            }
        }.run(tr)
    }
    
    def expectThrow[T](throwClass: Class[T])(f: => Unit): Unit = {
        try {
            f
        } catch {
            case x => {
                if (! throwClass.isAssignableFrom(x.getClass)) {
                    fail("Unexpected exception: " + x)
                }
            }
            return
        }
        fail("Expected exception " + throwClass.getName)
    }
    
    def expect(expected: Any)(f: => Any): Unit = {
        val actual = f
        if (actual != expected) {
            throw new AssertionError("expected '" + expected + "', got '" + actual + "'")
        }
    }
    
    private val _folderName = new ThreadLocal[File]
    
    // recursively delete a folder. should be built in. bad java.
    private def deleteFolder(folder: File): Unit = {
        for (val f <- folder.listFiles) {
            if (f.isDirectory) {
                deleteFolder(f)
            } else {
                f.delete
            }
        }
        folder.delete
    }
    
    private def runUnitTest(f: => Unit): Unit = {
        // make a temporary folder for this test
        var folder: File = null
        do {
            folder = new File("./scala-test-" + System.currentTimeMillis)
        } while (! folder.mkdir)
        _folderName.set(folder)

        var success = false
        try {
            setUp
            f
            success = true
        } finally {
            tearDown
        }
        
        // if the test failed, leave the folder around for debugging.
        if (success) {
            deleteFolder(folder)
        }
    }
    
    def currentFolder = { _folderName.get }
}
