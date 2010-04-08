package net.lag.kestrel.memcache

import scala.collection.mutable
import org.apache.mina.core.buffer.IoBuffer
import org.apache.mina.core.session.{IdleStatus, IoSession}
import org.apache.mina.filter.codec._
import net.lag.extensions._
import net.lag.naggati.{Decoder, End, ProtocolError, Step}
import net.lag.naggati.Steps._

object BinaryCodec extends Codec {
  val encoder = null
  val decoder = null
}
