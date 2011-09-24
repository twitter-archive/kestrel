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
import java.nio.channels.ClosedChannelException
import scala.collection.mutable
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.naggati.ProtocolError
import com.twitter.util.{Duration, Time}
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.ChannelGroup
import org.jboss.netty.handler.timeout._

/**
 * Kestrel handler that uses netty. Wraps up common netty code so you only receive a
 * message or an exception.
 */
abstract class NettyHandler[M](
  val channelGroup: ChannelGroup,
  queueCollection: QueueCollection,
  maxOpenTransactions: Int,
  clientTimeout: Option[Duration])
extends KestrelHandler(queueCollection, maxOpenTransactions) with ChannelUpstreamHandler {
  val log = Logger.get(getClass.getName)

  private var remoteAddress: InetSocketAddress = null
  var channel: Channel = null

  protected def clientDescription: String = {
    "%s:%d".format(remoteAddress.getHostName, remoteAddress.getPort)
  }

  def handleUpstream(context: ChannelHandlerContext, event: ChannelEvent) {
    event match {
      case m: MessageEvent =>
        handle(m.getMessage().asInstanceOf[M])
      case e: ExceptionEvent =>
        e.getCause() match {
          case _: ProtocolError =>
            handleProtocolError()
          case e: ClosedChannelException =>
            finish()
          case e: IOException =>
            log.debug("I/O Exception on session %d: %s", sessionId, e.toString)
          case e =>
            log.error(e, "Exception caught on session %d: %s", sessionId, e.toString)
            handleException(e)
        }
        e.getChannel().close()
      case s: ChannelStateEvent =>
        if ((s.getState() == ChannelState.CONNECTED) && (s.getValue() eq null)) {
          finish()
        } else if ((s.getState() == ChannelState.OPEN) && (s.getValue() == true)) {
          channel = s.getChannel()
          remoteAddress = channel.getRemoteAddress.asInstanceOf[InetSocketAddress]
          if (clientTimeout.isDefined) {
            channel.getPipeline.addFirst("idle", new IdleStateHandler(Kestrel.kestrel.timer, 0, 0, clientTimeout.get.inSeconds.toInt))
          }
          channelGroup.add(channel)
          // don't use `remoteAddress.getHostName` because it may do a DNS lookup.
          log.debug("New session %d from %s:%d", sessionId, remoteAddress.getAddress.getHostAddress, remoteAddress.getPort)
        }
      case i: IdleStateEvent =>
        log.debug("Idle timeout on session %s", channel)
        channel.close()
      case e =>
        context.sendUpstream(e)
    }
  }

  protected def handle(message: M)
  protected def handleProtocolError()
  protected def handleException(e: Throwable)
}
