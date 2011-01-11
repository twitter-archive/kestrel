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

import java.io.IOException
import java.net.InetSocketAddress
import scala.collection.mutable
import com.twitter.actors.Actor
import com.twitter.actors.Actor._
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.naggati.{NettyMessage, ProtocolError}
import com.twitter.util.{Duration, Time}
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.group.ChannelGroup
import org.jboss.netty.handler.timeout.IdleStateHandler

/**
 * Kestrel handler that uses netty/actors. Wraps up common netty/actor code so you only receive a
 * message or an exception.
 */
abstract class NettyHandler[M](
  val channel: Channel,
  val channelGroup: ChannelGroup,
  queueCollection: QueueCollection,
  maxOpenTransactions: Int,
  clientTimeout: Option[Duration])
extends KestrelHandler(queueCollection, maxOpenTransactions) with Actor {
  val log = Logger.get(getClass.getName)

  private val remoteAddress = channel.getRemoteAddress.asInstanceOf[InetSocketAddress]

  if (clientTimeout.isDefined) {
    channel.getPipeline.addFirst("idle", new IdleStateHandler(Kestrel.kestrel.timer, 0, 0, clientTimeout.get.inSeconds.toInt))
  }

  channelGroup.add(channel)
  log.debug("New session %d from %s:%d", sessionId, remoteAddress.getHostName, remoteAddress.getPort)
  start()

  protected def clientDescription: String = {
    "%s:%d".format(remoteAddress.getHostName, remoteAddress.getPort)
  }

  def act = {
    loop {
      react {
        case NettyMessage.MessageReceived(message) =>
          handle(message.asInstanceOf[M])

        case NettyMessage.ExceptionCaught(cause) =>
          cause match {
            case _: ProtocolError =>
              handleProtocolError()
            case _: IOException =>
              log.debug("I/O Exception on session %d: %s", sessionId, cause.toString)
            case e =>
              log.error(cause, "Exception caught on session %d: %s", sessionId, cause.toString)
              handleException(e)
          }
          channel.close()

        case NettyMessage.ChannelDisconnected() =>
          finish()
          exit()

        case NettyMessage.ChannelIdle(status) =>
          log.debug("Idle timeout on session %s", channel)
          channel.close()
      }
    }
  }

  protected def handle(message: M)
  protected def handleProtocolError()
  protected def handleException(e: Throwable)
}