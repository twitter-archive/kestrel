package com.twitter

import scala.testing.SUnit._

import com.twitter.scarling._
import net.lag.logging.Logger


object TestRunner {
    def sum(x: Seq[Int]) = (0 /: x) { _ + _ }
        
    def main(args:Array[String]): Unit = {
        // turn down logging
        Logger.get("").setLevel(Logger.FATAL)
        val results = new TestResult

        val suite = new TestSuite(PersistentQueueTests,
                                  QueueCollectionTests,
                                  memcache.MemCacheCodecTests)
                                      
        val testCount = sum(for (t <- suite.buf) yield t.asInstanceOf[sorg.testing.Tests].tests.length)
        Console.println("Running " + testCount + " unit tests:")
        suite.run(results)

        if (! results.failures.hasNext) {
            Console.println("Success!");
        } else {
            Console.println
            Console println("FAILED TESTS (" + results.failureCount + "):");
            Console.println
            for (val each:TestFailure <- results.failures) {
                Console.println(each.toString + ":")
                for (val line <- each.trace.split("\n")) {
                    Console.println("    " + line)
                }
                Console.println
            }
            System.exit(1)
        }
    }

}
