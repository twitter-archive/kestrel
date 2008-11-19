package com.twitter.scarling

import java.io._
import java.net.Socket
import scala.collection.Map
import scala.collection.mutable


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

  private def readline = {
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

  def get(key: String): String = {
    out.write(("get " + key + "\r\n").getBytes)
    val line = readline
    if (line == "END") {
      return ""
    }
    // VALUE <name> <flags> <length>
    val len = line.split(" ")(3).toInt
    val buffer = new Array[Byte](len)
    in.readFully(buffer)
    readline
    readline // "END"
    new String(buffer)
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
