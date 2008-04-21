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
        tests = Pair(desc, () => {
            setUp
            try {
                t
            } finally {
                tearDown
            }
        }) :: tests
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

    def withTempFolder(f: (String) => Any): Unit = {
        var folderName: String = null
        do {
            folderName = "/tmp/scala-test-" + System.currentTimeMillis
        } while (! new File(folderName).mkdir) 
        try {
            f(folderName)
        } finally {
            for (val filename <- new File(folderName).listFiles) {
                filename.delete
            }
            new File(folderName).delete
        }
    }
}
