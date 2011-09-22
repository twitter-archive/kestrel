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

package net.lag.kestrel

import java.nio.{ByteBuffer, ByteOrder}
import com.twitter.util.Time
import java.io.IOException

case class QItem(addTime: Time, expiry: Option[Time], data: Array[Byte], var xid: Int) {
  final def pack(opcode: Byte): ByteBuffer = pack(opcode, 0)

  final def pack(opcode: Byte, xid: Int): ByteBuffer = {
    val buffer = ByteBuffer.allocate(data.length + 21 + (if (xid == 0) 0 else 4))
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.put(opcode)
    if (xid != 0) buffer.putInt(xid)
    buffer.putInt(data.length + 16)
    buffer.putLong(addTime.inMilliseconds)
    if (expiry.isDefined) {
      buffer.putLong(expiry.get.inMilliseconds)
    } else {
      buffer.putLong(0)
    }
    buffer.put(data)
    buffer.flip()
    buffer
  }
}

object QItem {
  def unpack(data: Array[Byte]): QItem = {
    val dataLength = data.length
    if (dataLength < 16) {
      throw new IOException("Data unexpectedly short (< 16 bytes); length = " + dataLength)
    }
    val buffer = ByteBuffer.wrap(data)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val addTime = Time.fromMilliseconds(buffer.getLong)
    val expiry = buffer.getLong
    val bytes = new Array[Byte](dataLength - 16)
    buffer.get(bytes)
    QItem(addTime, if (expiry == 0) None else Some(Time.fromMilliseconds(expiry)), bytes, 0)
  }

  def unpackOldAdd(data: Array[Byte]): QItem = {
    val dataLength = data.length
    if (dataLength < 4) {
      throw new IOException("Data unexpectedly short (< 4 bytes); length = " + dataLength)
    }
    val buffer = ByteBuffer.wrap(data)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val expiry = buffer.getInt
    val bytes = new Array[Byte](dataLength - 4)
    buffer.get(bytes)
    QItem(Time.now, if (expiry == 0) None else Some(Time.fromSeconds(expiry)), bytes, 0)
  }
}
