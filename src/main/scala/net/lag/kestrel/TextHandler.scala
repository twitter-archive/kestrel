/*
 * Copyright 2010 Twitter, Inc.
 * Copyright 2010 Robey Pointer <robeypointer@gmail.com>
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

import scala.collection.mutable
import com.twitter.concurrent.ChannelSource
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.naggati._
import codec.{MemcacheResponse, MemcacheRequest}
import com.twitter.naggati.Stages._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import java.net.InetSocketAddress
import com.twitter.finagle.{ClientConnection, Service}
import com.twitter.util.{Future, Duration, Time}

object TextCodec {
  val MAX_PUT_BUFFER = 1024

  def apply(bytesRead: Int => Unit, bytesWritten: Int => Unit) = new Codec(read, write,
    bytesRead, bytesWritten)

  val read = readLine(true, "ISO-8859-1") { line =>
    if (line.endsWith(":")) {
      val segments = line.substring(0, line.length - 1).split(" ")
      readData(segments, new mutable.ListBuffer[Array[Byte]])
    } else {
      val segments = line.split(" ")
      emit(TextRequest(segments(0).toLowerCase, segments.drop(1).toList, Nil))
    }
  }

  private def readData(segments: Seq[String], items: mutable.ListBuffer[Array[Byte]]): Stage = readLine(true, "ISO-8859-1") { line =>
    if (line == "" || (items.size >= MAX_PUT_BUFFER)) {
      emit(TextRequest(segments(0).toLowerCase, segments.drop(1).toList, items.toList))
    } else {
      items += line.getBytes("UTF-8")
      readData(segments, items)
    }
  }

  val write = new Encoder[TextResponse] {
    def encode(response: TextResponse) = response.toBuffer
  }
}

case class TextRequest(command: String, args: List[String], items: List[Array[Byte]])

object TextResponse {
  val NO_ITEM = Some(ChannelBuffers.wrappedBuffer("*\n".getBytes))
  val COLON = ':'.toByte
  val LF = '\n'.toByte
}

abstract class TextResponse extends Codec.Signalling {
  def toBuffer: Option[ChannelBuffer]
}
case class ItemResponse(data: Option[Array[Byte]]) extends TextResponse {
  def toBuffer = {
    if (data.isDefined) {
      val bytes = data.get
      val buffer = ChannelBuffers.buffer(bytes.size + 2)
      buffer.writeByte(TextResponse.COLON)
      buffer.writeBytes(bytes)
      buffer.writeByte(TextResponse.LF)
      Some(buffer)
    } else {
      TextResponse.NO_ITEM
    }
  }

  override def toString = "<ItemResponse: %s>".format(data.map { _.toString })
}

case class ErrorResponse(message: String) extends TextResponse {
  def toBuffer = Some(ChannelBuffers.wrappedBuffer(("-" + message + "\n").getBytes("ascii")))
  override def toString = "<ErrorResponse: %s>".format(message)
}
case class CountResponse(count: Long) extends TextResponse {
  def toBuffer = Some(ChannelBuffers.wrappedBuffer(("+" + count.toString + "\n").getBytes("ascii")))
  override def toString = "<CountResponse: %s>".format(count)
}
case class NoResponse() extends TextResponse {
  def toBuffer = None
}
case class StringResponse(message: String) extends TextResponse {
  def toBuffer = Some(ChannelBuffers.wrappedBuffer((":" + message + "\n").getBytes("ascii")))
  override def toString = "<StringResponse: %s>".format(message)
}

/**
 * Simple text-line protocol handler for a kestrel connection.
 */
class TextHandler(
  connection: ClientConnection,
  queueCollection: QueueCollection,
  maxOpenReads: Int
) extends Service[TextRequest, TextResponse] {
  val log = Logger.get(getClass)

  val sessionId = Kestrel.sessionId.incrementAndGet()
  val handler = new KestrelHandler(queueCollection, maxOpenReads, clientDescription _, sessionId) with SimplePendingReads
  log.debug("New text session %d from %s", sessionId, clientDescription)

  protected def clientDescription: String = {
    val address = connection.remoteAddress.asInstanceOf[InetSocketAddress]
    "%s:%d".format(address.getAddress.getHostAddress, address.getPort)
  }

  override def release() {
    handler.finish()
    super.release()
  }

  def apply(request: TextRequest) = {
    request.command match {
      case "put" =>
        // put <queue> [expiry]:
        if (request.args.size < 1) {
          Future(ErrorResponse("Queue name required."))
        } else {
          val queueName = request.args(0)
          try {
            val expiry = request.args.drop(1).headOption.map { Time.now + _.toInt.milliseconds }
            var count = 0
            request.items.foreach { item =>
              if (handler.setItem(queueName, 0, expiry, item)) count += 1
            }
            Future(CountResponse(count))
          } catch {
            case e: NumberFormatException =>
              Future(ErrorResponse("Error parsing expiration time."))
          }
        }
      case "get" =>
        // get <queue> [timeout]
        if (request.args.size < 1) {
          Future(ErrorResponse("Queue name required."))
        } else {
          val queueName = request.args(0)
          try {
            val timeout = request.args.drop(1).headOption.map { _.toInt.milliseconds.fromNow }
            handler.closeAllReads(queueName)
            handler.getItem(queueName, timeout, true, false).map { item =>
              ItemResponse(item.map { _.data })
            }
          } catch {
            case e: NumberFormatException =>
              Future(ErrorResponse("Error parsing timeout."))
            case e: TooManyOpenReadsException =>
              Future(ErrorResponse("Too many open transactions; limit=" + maxOpenReads))
          }
        }
      case "peek" =>
        // peek <queue> [timeout]
        if (request.args.size < 1) {
          Future(ErrorResponse("Queue name required."))
        } else {
          val queueName = request.args(0)
          try {
            val timeout = request.args.drop(1).headOption.map { _.toInt.milliseconds.fromNow }
            handler.closeAllReads(queueName)
            handler.getItem(queueName, timeout, false, true).map { item =>
              ItemResponse(item.map { _.data })
            }
          } catch {
            case e: NumberFormatException =>
              Future(ErrorResponse("Error parsing timeout."))
          }
        }
      case "monitor" =>
        // monitor <queue> <timeout>
        if (request.args.size < 2) {
          Future(ErrorResponse("Queue name & timeout required."))
        } else {
          val queueName = request.args(0)
          val timeout = request.args(1).toInt.milliseconds.fromNow
          handler.closeAllReads(queueName)
          val channel = new LatchedChannelSource[TextResponse]
          handler.monitorUntil(queueName, Some(timeout), maxOpenReads, true) { (itemOption, _) =>
            itemOption match {
              case None =>
                channel.send(ItemResponse(None) then Codec.EndStream)
              case Some(item) =>
                channel.send(ItemResponse(Some(item.data)))
            }
          }
          Future(new NoResponse() then Codec.Stream(channel))
        }
      case "confirm" =>
        // confirm <queue> <count>
        if (request.args.size < 2) {
          Future(ErrorResponse("Queue name & count required."))
        } else {
          val queueName = request.args(0)
          val count = request.args(1).toInt
          if (handler.closeReads(queueName, count)) {
            Future(CountResponse(count))
          } else {
            Future(ErrorResponse("Not that many transactions open."))
          }
        }
      case "flush" =>
        if (request.args.size < 1) {
          Future(ErrorResponse("Queue name required."))
        } else {
          handler.flush(request.args(0))
          Future(CountResponse(0))
        }
      case "delete" =>
        if (request.args.size < 1) {
          Future(ErrorResponse("Queue name required."))
        } else {
          handler.delete(request.args(0))
          Future(CountResponse(0))
        }
      case "quit" =>
        connection.close()
        Future(CountResponse(0))
      case "shutdown" =>
        handler.shutdown()
        Future(CountResponse(0))
      case "version" =>
        Future(StringResponse(Kestrel.runtime.jarVersion))
      case x =>
        Future(ErrorResponse("Unknown command: " + x))
    }
  }
}
