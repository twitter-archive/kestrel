/*
 * Copyright 2009 Twitter, Inc.
 * Copyright 2009 Robey Pointer <robeypointer@gmail.com>
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

package net.lag.kestrel.memcache

import org.apache.mina.core.buffer.IoBuffer
import net.lag.extensions._
import net.lag.naggati.ProtocolError


case class Request(line: List[String], data: Option[Array[Byte]]) {
  override def toString = {
    "<Request: " + line.mkString("[", " ", "]") + (data match {
      case None => ""
      case Some(x) => ": " + x.hexlify
    }) + ">"
  }
}

case class Response(data: IoBuffer)

object Codec {
  def encoderFor(protocol: String) = {
    protocol match {
      case "ascii" => memcache.ASCIICodec.encoder
      case "binary" => memcache.BinaryCodec.encoder
      case _ => throw new ProtocolError("Invalid protocol: " + protocol)
    }
  }

  def decoderFor(protocol: String) = {
    protocol match {
      case "ascii" => memcache.ASCIICodec.decoder
      case "binary" => memcache.BinaryCodec.decoder
      case _ => throw new ProtocolError("Invalid protocol: " + protocol)
    }
  }
}

class Codec {
  // See ASCII and Binary for their respective implementations
}
