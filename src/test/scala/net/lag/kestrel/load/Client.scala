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
      p.writeMessageBegin(new TMessage("flush", TMessageType.CALL, 0))
      (new thrift.Kestrel.flush_args(queueName)).write(p)
    }
  }

  def flushSuccess() = {
    withProtocol { p =>
      p.writeMessageBegin(new TMessage("flush", TMessageType.REPLY, 0))
      (new thrift.Kestrel.flush_result()).write(p)
    }
  }

  def get(queueName: String, timeoutMsec: Option[Int]) = {
    withProtocol { p =>
      p.writeMessageBegin(new TMessage("get", TMessageType.CALL, 0))
      (new thrift.Kestrel.get_args(queueName, 1, timeoutMsec.getOrElse(0), true)).write(p)
    }
  }

  def getEmpty(queueName: String) = {
    withProtocol { p =>
      p.writeMessageBegin(new TMessage("get", TMessageType.REPLY, 0))
      (new thrift.Kestrel.get_result(success = Some(Nil))).write(p)
    }
  }

  def getSuccess(queueName: String, data: String) = {
    withProtocol { p =>
      p.writeMessageBegin(new TMessage("get", TMessageType.REPLY, 0))
      val item = new thrift.Item(ByteBuffer.wrap(data.getBytes), 0)
      (new thrift.Kestrel.get_result(success = Some(Seq(item)))).write(p)
    }
  }
}
