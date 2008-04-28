package com.twitter.scarling.memcache

import scala.collection.mutable
import org.apache.mina.common.{ByteBuffer, IoSession}
import org.apache.mina.filter.codec._
import org.apache.mina.filter.codec.demux._


case class Request(line: List[String], data: Option[Array[Byte]])
case class Response(data: ByteBuffer)


/**
 * Kind of arcane way to get Mina to translate incoming data into memcache
 * request objects. Outgoing data is always a wrapped ByteBuffer.
 */
class ProtocolCodecFactory extends DemuxingProtocolCodecFactory {
    register(classOf[Decoder])
    register(classOf[Encoder])
}


/**
 * Protocol encoder for a memcache server.
 * Inspired by jmemcached.
 */
class Encoder extends MessageEncoder {
    def encode(session: IoSession, message: AnyRef, out: ProtocolEncoderOutput) = {
        val buffer = message.asInstanceOf[Response].data
        Scarling.addBytesWritten(buffer.remaining)
        out.write(buffer)
    }
    
    def getMessageTypes: java.util.Set[Class[_]] = java.util.Collections.singleton(classOf[Response])
}


/**
 * Protocol decoder for a memcache server.
 * Inspired by jmemcached.
 */
class Decoder extends MessageDecoderAdapter {
    private class State {
        var buffer = new mutable.ArrayBuffer[Byte]
        var line: Option[Array[String]] = None
        var dataBytes = 0
        
        def reset = {
            line = None
            dataBytes = 0
        }
    }
    
    
    private val STATE_KEY = "scala.mina.memcache.state"
    private val KNOWN_COMMANDS = List("GET", "SET", "STATS", "SHUTDOWN")
    private val DATA_COMMANDS = List("SET")
    
    
    def decodable(session: IoSession, in: ByteBuffer): MessageDecoderResult = {
        var state = session.getAttribute(STATE_KEY).asInstanceOf[State]
        if (state == null) {
            state = new State
            session.setAttribute(STATE_KEY, state)
        }

        state.line match {
            case None => MessageDecoderResult.OK
            case Some(_) => {
                if (in.remaining >= state.dataBytes) {
                    MessageDecoderResult.OK
                } else {
                    MessageDecoderResult.NEED_DATA
                }
            }
        }
    }
    
    def decode(session: IoSession, in: ByteBuffer, out: ProtocolDecoderOutput): MessageDecoderResult = {
        var state = session.getAttribute(STATE_KEY).asInstanceOf[State]
        if (state == null) {
            state = new State
            session.setAttribute(STATE_KEY, state)
        }
        
        val bytes = new Array[Byte](in.remaining)
        in.get(bytes)
        state.buffer ++= bytes
        Scarling.addBytesRead(bytes.length)
        
        state.line match {
            case None => decodeLine(state, out)
            case Some(_) => decodeData(state, out)
        }
    }
    
    private def decodeLine(state: State, out: ProtocolDecoderOutput): MessageDecoderResult = {
        val lf = state.buffer.indexOf('\n')
        if (lf > 0) {
            var end = lf
            if (state.buffer(end - 1) == '\r') {
                end -= 1
            }
            val line = new String(state.buffer.slice(0, end).toArray, "ISO-8859-1")
            state.buffer.trimStart(lf + 1)
            val segments = line.split(" ")
            segments(0) = segments(0).toUpperCase
            
            state.line = Some(segments)
            val command = segments(0)
            if (! KNOWN_COMMANDS.contains(command)) {
                MessageDecoderResult.NOT_OK
            } else {
                if (DATA_COMMANDS.contains(command)) {
                    if (state.line.get.length < 5) {
                        MessageDecoderResult.NOT_OK
                    } else {
                        state.dataBytes = segments(4).toInt + 2
                        decodeData(state, out)
                    }
                } else {
                    out.write(Request(state.line.get.toList, None))
                    state.reset
                    MessageDecoderResult.OK
                }
            }
        } else {
            // we ate the current buffer, but don't have a full line yet.
            MessageDecoderResult.OK
        }
    }
    
    private def decodeData(state: State, out: ProtocolDecoderOutput): MessageDecoderResult = {
        if (state.buffer.length < state.dataBytes) {
            // still need more.
            MessageDecoderResult.NEED_DATA
        } else {
            // final 2 bytes are just "\r\n" mandated by protocol.
            val data = state.buffer.slice(0, state.dataBytes - 2)
            out.write(Request(state.line.get.toList, Some(data.toArray)))
            state.buffer.trimStart(state.dataBytes)
            state.reset
            MessageDecoderResult.OK
        }
    }
}
