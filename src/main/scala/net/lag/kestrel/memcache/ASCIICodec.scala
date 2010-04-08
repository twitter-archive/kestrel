package net.lag.kestrel.memcache

import scala.collection.mutable
import org.apache.mina.core.buffer.IoBuffer
import org.apache.mina.core.session.{IdleStatus, IoSession}
import org.apache.mina.filter.codec._
import net.lag.extensions._
import net.lag.naggati.{Decoder, End, ProtocolError, Step}
import net.lag.naggati.Steps._

object ASCIICodec extends Codec {
  private val KNOWN_COMMANDS = List("GET", "SET", "STATS", "SHUTDOWN", "RELOAD", "FLUSH", "FLUSH_ALL",
    "DUMP_CONFIG", "DUMP_STATS", "DELETE", "FLUSH_EXPIRED", "FLUSH_ALL_EXPIRED", "VERSION")
  private val DATA_COMMANDS = List("SET")

  val encoder = new ProtocolEncoder {
    def encode(session: IoSession, message: AnyRef, out: ProtocolEncoderOutput) = {
      val buffer = message.asInstanceOf[Response].data
      KestrelStats.bytesWritten.incr(buffer.remaining)
      out.write(buffer)
    }

    def dispose(session: IoSession): Unit = {
      // nothing.
    }
  }

  val decoder = new Decoder(readLine(true, "ISO-8859-1") { line =>
    KestrelStats.bytesRead.incr(line.length + 1)
    val segments = line.split(" ")
    segments(0) = segments(0).toUpperCase

    val command = segments(0)
    if (! KNOWN_COMMANDS.contains(command)) {
      throw new ProtocolError("Invalid command: " + command)
    }

    if (DATA_COMMANDS.contains(command)) {
      if (segments.length < 5) {
        throw new ProtocolError("Malformed request line")
      }
      val dataBytes = segments(4).toInt
      readBytes(dataBytes + 2) {
        KestrelStats.bytesRead.incr(dataBytes + 2)
        // final 2 bytes are just "\r\n" mandated by protocol.
        val bytes = new Array[Byte](dataBytes)
        state.buffer.get(bytes)
        state.buffer.position(state.buffer.position + 2)
        state.out.write(Request(segments.toList, Some(bytes)))
        End
      }
    } else {
      state.out.write(Request(segments.toList, None))
      End
    }
  })
}
