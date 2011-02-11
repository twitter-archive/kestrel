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

case class QItem(addTime: Long, expiry: Option[Time], data: Array[Byte], var xid: Int) {
  def pack(opcode: Byte, withXid: Boolean): ByteBuffer = {
    val headerSize = if (withXid) 9 else 5
    val buffer = ByteBuffer.allocate(data.length + 16 + headerSize)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.put(opcode)
    if (withXid) {
      buffer.putInt(xid)
    }
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
    val buffer = ByteBuffer.wrap(data)
    val bytes = new Array[Byte](data.length - 16)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val addTime = Time.fromMilliseconds(buffer.getLong)
    val expiry = buffer.getLong
    buffer.get(bytes)
    QItem(addTime, if (expiry == 0) None else Some(Time.fromMilliseconds(expiry)), bytes, 0)
  }

  def unpackOldAdd(data: Array[Byte]): QItem = {
    val buffer = ByteBuffer.wrap(data)
    val bytes = new Array[Byte](data.length - 4)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val expiry = buffer.getInt
    buffer.get(bytes)
    QItem(Time.now, if (expiry == 0) None else Some(Time.fromSeconds(expiry)), bytes, 0)
  }
}
