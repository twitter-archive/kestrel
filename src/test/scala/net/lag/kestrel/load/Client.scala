/*
 * Copyright 2009 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.lag.kestrel.load

import net.lag.kestrel.thrift._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.Service
import java.net.InetSocketAddress
import org.apache.thrift.protocol.TBinaryProtocol
import com.twitter.finagle.thrift.{ThriftClientFramedCodec, ThriftClientRequest}
import java.nio.ByteBuffer
import java.nio.channels._

object Client {
  def create(protocolName: String, hostname: String, port: Int) = {
    if (protocolName == "memcache") {
      new MemcacheClient(hostname, port)
    } else if (protocolName == "thrift") {
      new ThriftClient(hostname, port)
    } else {
      throw new Exception("Invalid protocol name. Must equal 'memcache' or 'thrift'.")
    }
  }
}

trait Client {
    def put(queueName: String, n: Int, data: String)
    def flush(queueName: String)
    def release() {}
}

class MemcacheClient(hostname: String, port: Int) extends Client {

    private val STORED = ByteBuffer.wrap("STORED\r\n".getBytes)
    private val END = ByteBuffer.wrap("END\r\n".getBytes)
    private val socket = SocketChannel.open(new InetSocketAddress(hostname, port))

    def put(queueName: String, n: Int, data: String) {
      val spam = ByteBuffer.wrap(("set " + queueName + " 0 0 " + data.length + "\r\n" + data + "\r\n").getBytes)
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
        STORED.rewind
        if (buffer != STORED) {
          // the "!" is important.
          throw new Exception("Unexpected response at " + i + "!")
        }
      }
    }

    def flush(queueName: String) {
      val spam = ByteBuffer.wrap(("FLUSH " + queueName + "\r\n").getBytes)
      val buffer = ByteBuffer.allocate(5)

      spam.rewind
      while (spam.position < spam.limit) {
        socket.write(spam)
      }
      buffer.rewind
      while (buffer.position < buffer.limit) {
        socket.read(buffer)
      }
      buffer.rewind
      END.rewind
      if (buffer != END) {
        throw new Exception("Unexpected response.")
      }
    }

    override def release() {
      socket.close()
    }
}

class ThriftClient(hostname: String, port: Int) extends Client {

    val address = new InetSocketAddress(hostname, port)

    val service: Service[ThriftClientRequest, Array[Byte]] = ClientBuilder()
      .hosts(address).codec(ThriftClientFramedCodec()).hostConnectionLimit(100).build()

    val client = new net.lag.kestrel.thrift.Kestrel.FinagledClient(service, new TBinaryProtocol.Factory())

    val batchSize = 1000

    def put(queueName: String, n: Int, data: String) {
      val rawData = ByteBuffer.wrap(data.getBytes)

      val batchRawData = List.range(0,batchSize).map((x) => rawData)
      
      val putfn = (n: Int) => {
        val count : Int = client.put(queueName, batchRawData.take(n))()
        if(count != n) {
            throw new Exception("Failed to put " + (n - count) + " items on the queue.")
        }
      }

      for (i <- 1 to n/batchSize) putfn(batchSize)
      putfn(n%batchSize)
    }

    def flush(queueName: String) {
      client.flush(queueName)
    }

    override def release() {
      service.release()
    }
}
// vim: set ts=4 sw=4 et:
