/*
 * Copyright 2011 Twitter, Inc.
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

package net.lag.kestrel
package load

import java.nio.ByteBuffer

/**
 * Abstraction for generating byte buffers of requests for different protocols.
 *
 * The methods may do a fair amount of work, so the idea is to save the returned ByteBuffer and
 * use it repeatedly.
 */
trait Client {
  def put(queueName: String, data: String): ByteBuffer
  def putSuccess(): ByteBuffer

  def putN(queueName: String, data: Seq[String]): ByteBuffer
  def putNSuccess(count: Int): ByteBuffer

  def flush(queueName: String): ByteBuffer
  def flushSuccess(): ByteBuffer

  def get(queueName: String, timeoutMsec: Option[Int]): ByteBuffer
  def getEmpty(queueName: String): ByteBuffer
  def getSuccess(queueName: String, data: String): ByteBuffer

  def getN(queueName: String, timeoutMsec: Option[Int], maxItems: Int): ByteBuffer
  def getNSuccess(queueName: String, data: Seq[String]): ByteBuffer

  // "monitor" can return either a single response (monitorSuccess) or a stream of single "get"
  // responses followed by a "getEmpty".
  def monitorHasMultipleResponses: Boolean
  def monitor(queueName: String, timeoutMsec: Int, maxItems: Int): ByteBuffer
  def monitorSuccess(queueName: String, data: Seq[String]): ByteBuffer
  def confirmMonitor(queueName: String, items: Int): ByteBuffer
  def confirmMonitorSuccess(queueName: String, items: Int): ByteBuffer
}

object MemcacheClient extends Client {
  def put(queueName: String, data: String) = {
    ByteBuffer.wrap(("set " + queueName + " 0 0 " + data.length + "\r\n" + data + "\r\n").getBytes)
  }

  def putSuccess() = {
    ByteBuffer.wrap("STORED\r\n".getBytes)
  }

  def putN(queueName: String, data: Seq[String]) = {
    ByteBuffer.wrap(data.map { item =>
      "set " + queueName + " 0 0 " + item.length + "\r\n" + item + "\r\n"
    }.mkString.getBytes)
  }

  def putNSuccess(count: Int) = {
    ByteBuffer.wrap((0 until count).map { _ => "STORED\r\n" }.mkString.getBytes)
  }

  def flush(queueName: String) = {
    ByteBuffer.wrap(("flush " + queueName + "\r\n").getBytes)
  }

  def flushSuccess() = {
    ByteBuffer.wrap("END\r\n".getBytes)
  }

  def get(queueName: String, timeoutMsec: Option[Int]) = {
    ByteBuffer.wrap(("get " + queueName + timeoutMsec.map { "/t=" + _ }.getOrElse("") + "\r\n").getBytes)
  }

  def getEmpty(queueName: String) = {
    ByteBuffer.wrap("END\r\n".getBytes)
  }

  def getSuccess(queueName: String, data: String) = {
    ByteBuffer.wrap(("VALUE " + queueName + " 0 " + data.length + "\r\n" + data + "\r\nEND\r\n").getBytes)
  }

  def getN(queueName: String, timeoutMsec: Option[Int], maxItems: Int) = {
    ByteBuffer.wrap((0 until maxItems).map { _ =>
      "get " + queueName + timeoutMsec.map { "/t=" + _ }.getOrElse("") + "\r\n"
    }.mkString.getBytes)
  }

  def getNSuccess(queueName: String, data: Seq[String]) = {
    ByteBuffer.wrap(data.map { d =>
      "VALUE " + queueName + " 0 " + d.length + "\r\n" + d + "\r\nEND\r\n"
    }.mkString.getBytes)
  }

  def monitorHasMultipleResponses = true

  def monitor(queueName: String, timeoutMsec: Int, maxItems: Int) = {
    ByteBuffer.wrap(("monitor " + queueName + " " + (timeoutMsec / 1000) + " " + maxItems + "\r\n").getBytes)
  }

  def monitorSuccess(queueName: String, data: Seq[String]) = {
    // memcache doesn't support monitorSuccess.
    ByteBuffer.wrap("ERROR".getBytes)
  }

  def confirmMonitor(queueName: String, items: Int) = {
    ByteBuffer.wrap(("confirm " + queueName + " " + items + "\r\n").getBytes)
  }

  def confirmMonitorSuccess(queueName: String, items: Int) = {
    ByteBuffer.wrap("END\r\n".getBytes)
  }
}

object ThriftClient extends Client {
  import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
  import java.util.Arrays
  import org.apache.thrift.protocol.{TBinaryProtocol, TMessage, TMessageType, TProtocol}
  import org.apache.thrift.transport.{TFramedTransport, TIOStreamTransport, TMemoryBuffer}

  private def withProtocol(f: TProtocol => Unit) = {
    val buffer = new TMemoryBuffer(512)
    val protocol = new TBinaryProtocol(new TFramedTransport(buffer))
    f(protocol)
    protocol.writeMessageEnd()
    protocol.getTransport().flush()
    ByteBuffer.wrap(Arrays.copyOfRange(buffer.getArray, 0, buffer.length))
  }

  def put(queueName: String, data: String) = putN(queueName, Seq(data))

  def putSuccess() = putNSuccess(1)

  def putN(queueName: String, data: Seq[String]) = {
    withProtocol { p =>
      p.writeMessageBegin(new TMessage("put", TMessageType.CALL, 0))
      val item = data.map { item => ByteBuffer.wrap(item.getBytes) }
      (new thrift.Kestrel.put_args(queueName, item, 0)).write(p)
    }
  }

  def putNSuccess(count: Int) = {
    withProtocol { p =>
      p.writeMessageBegin(new TMessage("put", TMessageType.REPLY, 0))
      (new thrift.Kestrel.put_result(success = Some(count))).write(p)
    }
  }

  def flush(queueName: String) = {
    withProtocol { p =>
      p.writeMessageBegin(new TMessage("flush_queue", TMessageType.CALL, 0))
      (new thrift.Kestrel.flushQueue_args(queueName)).write(p)
    }
  }

  def flushSuccess() = {
    withProtocol { p =>
      p.writeMessageBegin(new TMessage("flush_queue", TMessageType.REPLY, 0))
      (new thrift.Kestrel.flushQueue_result()).write(p)
    }
  }

  def get(queueName: String, timeoutMsec: Option[Int]) = getN(queueName, timeoutMsec, 1)

  def getEmpty(queueName: String) = getNSuccess(queueName, Nil)

  def getSuccess(queueName: String, data: String) = getNSuccess(queueName, Seq(data))

  def getN(queueName: String, timeoutMsec: Option[Int], maxItems: Int) =
    monitor(queueName, timeoutMsec.getOrElse(0), maxItems)

  def getNSuccess(queueName: String, data: Seq[String]) = monitorSuccess(queueName, data)

  def monitorHasMultipleResponses = false

  def monitor(queueName: String, timeoutMsec: Int, maxItems: Int) = {
    withProtocol { p =>
      p.writeMessageBegin(new TMessage("get", TMessageType.CALL, 0))
      (new thrift.Kestrel.get_args(queueName, maxItems, timeoutMsec, 0)).write(p)
    }
  }

  def monitorSuccess(queueName: String, data: Seq[String]) = {
    withProtocol { p =>
      p.writeMessageBegin(new TMessage("get", TMessageType.REPLY, 0))
      val items = data.map { item => new thrift.Item(ByteBuffer.wrap(item.getBytes), 0) }
      (new thrift.Kestrel.get_result(success = Some(items))).write(p)
    }
  }

  def confirmMonitor(queueName: String, items: Int): ByteBuffer = {
    // meaningless here
    ByteBuffer.wrap("".getBytes)
  }

  def confirmMonitorSuccess(queueName: String, items: Int): ByteBuffer = {
    // meaningless here
    ByteBuffer.wrap("".getBytes)
  }
}
