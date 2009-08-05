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

import _root_.java.io._
import _root_.java.net.Socket
import _root_.scala.collection.Map
import _root_.scala.collection.mutable


class ClientError(reason: String) extends Exception(reason)

class TestClient(host: String, port: Int) {

  var socket: Socket = null
  var out: OutputStream = null
  var in: DataInputStream = null

  connect


  def connect = {
    socket = new Socket(host, port)
    out = socket.getOutputStream
    in = new DataInputStream(socket.getInputStream)
  }

  def disconnect = {
    socket.close
  }

  def readline = {
    // this isn't meant to be efficient, just simple.
    val out = new StringBuilder
    var done = false
    while (!done) {
      val ch: Int = in.read
      if ((ch < 0) || (ch == 10)) {
        done = true
      } else if (ch != 13) {
        out += ch.toChar
      }
    }
    out.toString
  }

  def set(key: String, value: String): String = {
    out.write(("set " + key + " 0 0 " + value.length + "\r\n" + value + "\r\n").getBytes)
    readline
  }

  def set(key: String, value: String, expiry: Int) = {
    out.write(("set " + key + " 0 " + expiry + " " + value.length + "\r\n" + value + "\r\n").getBytes)
    readline
  }

  def setData(key: String, value: Array[Byte]) = {
    out.write(("set " + key + " 0 0 " + value.size + "\r\n").getBytes)
    out.write(value)
    out.write("\r\n".getBytes)
    readline
  }

  def getData(key: String): Array[Byte] = {
    out.write(("get " + key + "\r\n").getBytes)
    val line = readline
    if (line == "END") {
      return new Array[Byte](0)
    }
    if (! line.startsWith("VALUE ")) {
      throw new ClientError(line)
    }
    // VALUE <name> <flags> <length>
    val len = line.split(" ")(3).toInt
    val buffer = new Array[Byte](len)
    in.readFully(buffer)
    readline
    readline // "END"
    buffer
  }

  def get(key: String): String = {
    new String(getData(key))
  }

  def add(key: String, value: String) = {
    out.write(("add " + key + " 0 0 " + value.length + "\r\n" + value + "\r\n").getBytes)
    readline
  }

  def stats: Map[String, String] = {
    out.write("stats\r\n".getBytes)
    var done = false
    val map = new mutable.HashMap[String, String]
    while (!done) {
      val line = readline
      if (line startsWith "STAT") {
        val args = line.split(" ")
        map(args(1)) = args(2)
      } else if (line == "END") {
        done = true
      }
    }
    map
  }
}
