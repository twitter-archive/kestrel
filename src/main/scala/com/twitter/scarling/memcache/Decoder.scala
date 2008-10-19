package com.twitter.scarling.memcache

import scala.collection.mutable
import org.apache.mina.common.{ByteBuffer, IoSession}
import org.apache.mina.filter.codec._
import net.lag.extensions._


case class Request(line: List[String], data: Option[Array[Byte]]) {
    override def toString = {
        "<Request: " + line.mkString("[", " ", "]") + (data match {
            case None => ""
            case Some(x) => ": " + x.hexlify
        }) + ">"
    }
}

case class Response(data: ByteBuffer)

class ProtocolException(desc: String) extends Exception(desc)


/**
 * Protocol encoder for a memcache server.
 * Inspired by jmemcached.
 */
class Encoder extends ProtocolEncoder {
    def encode(session: IoSession, message: AnyRef, out: ProtocolEncoderOutput) = {
        val buffer = message.asInstanceOf[Response].data
        ScarlingStats.bytesWritten.incr(buffer.remaining)
        out.write(buffer)
    }

    def dispose(session: IoSession): Unit = {
        // nothing.
    }
}


/**
 * Protocol decoder for a memcache server.
 * Inspired by jmemcached.
 */
class Decoder extends ProtocolDecoder {
    private class State {
        var buffer = ByteBuffer.allocate(1024)
        buffer.setAutoExpand(true)

        var line: Option[Array[String]] = None
        var dataBytes = 0

        def reset = {
            line = None
            dataBytes = 0
            resetBuffer
        }

        // truncate the buffer, deleting what's already been read.
        // go into write mode.
        def resetBuffer = {
            if (buffer.position == 0) {
                // leave it alone
            } else if (buffer.position < buffer.limit) {
                val remainingBytes = new Array[Byte](buffer.limit - buffer.position)
                buffer.get(remainingBytes)
                buffer.clear
                buffer.put(remainingBytes)
            } else {
                buffer.clear
            }
        }

        // prepare a buffer for writes.
        def unflipBuffer = {
            buffer.position(buffer.limit)
            buffer.limit(buffer.capacity)
        }
    }


    private val STATE_KEY = "scala.mina.memcache.state"
    private val KNOWN_COMMANDS = List("GET", "SET", "STATS", "SHUTDOWN")
    private val DATA_COMMANDS = List("SET")


    def dispose(session: IoSession): Unit = {
        session.removeAttribute(STATE_KEY)
    }

    def finishDecode(session: IoSession, out: ProtocolDecoderOutput): Unit = {
        // um, no. :)
    }

    def decode(session: IoSession, in: ByteBuffer, out: ProtocolDecoderOutput): Unit = {
        var state = session.getAttribute(STATE_KEY).asInstanceOf[State]
        if (state == null) {
            state = new State
            session.setAttribute(STATE_KEY, state)
        }

        ScarlingStats.bytesRead.incr(in.remaining)
        state.buffer.put(in)
        state.buffer.flip

        state.line match {
            case None => decodeLine(state, out)
            case Some(_) => decodeData(state, out)
        }
    }

    private def decodeLine(state: State, out: ProtocolDecoderOutput): Unit = {
        val lf = bufferIndexOf(state.buffer, '\n')
        if (lf < 0) {
            state.unflipBuffer
            return
        }

        var end = lf
        if ((end > 0) && (state.buffer.get(end - 1) == '\r')) {
            end -= 1
        }

        // pull off the line into a string
        val lineBytes = new Array[Byte](end)
        state.buffer.get(lineBytes)
        val line = new String(lineBytes, "ISO-8859-1")
        state.buffer.position(state.buffer.position + (lf - end) + 1)

        val segments = line.split(" ")
        segments(0) = segments(0).toUpperCase

        state.line = Some(segments)
        val command = segments(0)
        if (! KNOWN_COMMANDS.contains(command)) {
            throw new ProtocolException("Invalid command: " + command)
        }

        if (DATA_COMMANDS.contains(command)) {
            if (state.line.get.length < 5) {
                throw new ProtocolException("Malformed request line")
            }
            state.dataBytes = segments(4).toInt + 2
            state.resetBuffer
            state.buffer.flip
            decodeData(state, out)
        } else {
            out.write(Request(state.line.get.toList, None))
            state.reset
        }
    }

    private def decodeData(state: State, out: ProtocolDecoderOutput): Unit = {
        if (state.buffer.remaining < state.dataBytes) {
            // still need more.
            state.unflipBuffer
            return
        }

        // final 2 bytes are just "\r\n" mandated by protocol.
        val bytes = new Array[Byte](state.dataBytes - 2)
        state.buffer.get(bytes)
        state.buffer.position(state.buffer.position + 2)

        out.write(Request(state.line.get.toList, Some(bytes)))
        state.reset
    }

    private def bufferIndexOf(buffer: ByteBuffer, b: Byte): Int = {
        var i = buffer.position
        while (i < buffer.limit) {
            if (buffer.get(i) == b) {
                return i - buffer.position
            }
            i += 1
        }
        return -1
    }
}
