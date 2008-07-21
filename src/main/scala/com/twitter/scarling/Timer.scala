package com.twitter.scarling

import scala.collection.Map
import scala.collection.mutable


class Timer(val name: String, val reportAt: Int) {
    var counter = 0
    var total = 0
    var sum: Long = 0
    
    def add(timing: Long) = synchronized {
        counter += 1
        total += 1
        sum += timing
        
        if (counter == reportAt) {
            val average: Double = sum.asInstanceOf[Double] / total
            Console.println("TIMER " + name + " = " + (average / 1000) + " usec")
            counter = 0
        }
    }
}


object Timer {
    private val timers = new mutable.HashMap[String, Timer]
    
    def get(name: String, count: Int): Timer = synchronized {
        timers.get(name) match {
            case None => { timers(name) = new Timer(name, count); timers(name) }
            case Some(t) => t
        }
    }
    
    def run[T](name: String, count: Int)(f: => T) = {
        val timer = get(name, count)
        val startTime = System.nanoTime
        val result = f
        val timing = System.nanoTime - startTime
        timer.add(timing)
        result
    }
}
