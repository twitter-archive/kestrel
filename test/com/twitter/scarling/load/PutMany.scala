package com.twitter.scarling.load

import java.net._
import java.nio._
import java.nio.channels._


/**
 * Spam a scarling server with 1M copies of a pop song lyric, to see how
 * quickly it can absorb them.
 */
object PutMany {
   private val LYRIC =
"crossed off, but never forgotten\n" +
"misplaced, but never losing hold\n" +
"these are the moments that bind us\n" +
"repressed, but never erased\n" +
"knocked down, but never giving up\n" +
"locked up where no one can find us\n" +
"we'll survive in here til the end\n" +
"\n" +
"there are no more fights to fight\n" +
"my trophies are the scars that will never heal\n" +
"but i get carried away sometimes\n" +
"i wake up in the night swinging at the ceiling\n" + 
"it's hard to leave old ways behind\n" +
"but harder when you think that's all there is\n" +
"don't look at me that way\n" +
"\n" +
"ignored, when your whole world's collapsed\n" +
"dismissed, before you speak a word\n" +
"these are the moments that bind you\n" +
"come clean, but everything's wrong\n" +
"sustained, but barely holding on\n" +
"run down, with no one to find you\n" +
"we're survivors, here til the end"

    private val EXPECT = ByteBuffer.wrap("STORED\r\n".getBytes)
    
    def put(socket: SocketChannel, queueName: String, n: Int) = {
        val spam = ByteBuffer.wrap(("set " + queueName + " 0 0 " + LYRIC.length + "\r\n" + LYRIC + "\r\n").getBytes)
        val buffer = ByteBuffer.allocate(8)
        for (i <- 0 until n) {
            spam.rewind
            while (spam.position < spam.limit) {
                socket.write(spam)
            }
            buffer.rewind
            while (buffer.position < buffer.limit) {
                socket.read(buffer)
            }
            buffer.rewind
            EXPECT.rewind
            if (buffer != EXPECT) {
                // the "!" is important.
                throw new Exception("Unexpected response at " + i + "!")
            }
        }
    }
    
    def main(args: Array[String]) = {
        var threadList: List[Thread] = Nil
        val startTime = System.currentTimeMillis
        
        // do 100 pushes each to 100 different queues = 10k pushes total
        for (i <- 0 until 100) {
            val t = new Thread {
                override def run = {
                    val socket = SocketChannel.open(new InetSocketAddress("localhost", 22122))
                    put(socket, "spam" + i, 100)
                }
            }
            threadList = t :: threadList
            t.start
        }
        for (t <- threadList) {
            t.join
        }
        
        // do 10k pushes to the same queue
        //val socket = SocketChannel.open(new InetSocketAddress("localhost", 22122))
        //put(socket, "spam", 10000)

        val endTime = System.currentTimeMillis
        Console.println("Finished in " + (endTime - startTime) + " msec.")
    }
}
