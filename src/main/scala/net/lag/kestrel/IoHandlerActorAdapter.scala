/*
 * Copyright (c) 2008 Robey Pointer <robeypointer@lag.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package net.lag.scarling

import scala.actors.Actor
import org.apache.mina.common._
import net.lag.logging.Logger


// Actor messages for the Mina "events"
abstract sealed class MinaMessage
object MinaMessage {
  case object SessionOpened extends MinaMessage
  case class MessageReceived(message: AnyRef) extends MinaMessage
  case class MessageSent(message: AnyRef) extends MinaMessage
  case class ExceptionCaught(cause: Throwable) extends MinaMessage
  case class SessionIdle(status: IdleStatus) extends MinaMessage
  case object SessionClosed extends MinaMessage
}


class IoHandlerActorAdapter(val actorFactory: (IoSession) => Actor) extends IoHandler {

  private val log = Logger.get
  private val ACTOR_KEY = "scala.mina.actor"

  private def actorFor(session: IoSession) = session.getAttribute(ACTOR_KEY).asInstanceOf[Actor]

  def sessionCreated(session: IoSession) = {
    val actor = actorFactory(session)
    session.setAttribute(ACTOR_KEY, actor)
  }

  def sessionOpened(session: IoSession) = actorFor(session) ! MinaMessage.SessionOpened
  def messageReceived(session: IoSession, message: AnyRef) = actorFor(session) ! new MinaMessage.MessageReceived(message)
  def messageSent(session: IoSession, message: AnyRef) = actorFor(session) ! new MinaMessage.MessageSent(message)

  def exceptionCaught(session: IoSession, cause: Throwable) = {
    actorFor(session) match {
      case null =>
        // weird bad: an exception happened but i guess it wasn't associated with any existing session.
        log.error(cause, "Exception inside mina!")
      case actor: Actor => actor ! new MinaMessage.ExceptionCaught(cause)
    }
  }

  def sessionIdle(session: IoSession, status: IdleStatus) = actorFor(session) ! new MinaMessage.SessionIdle(status)

  def sessionClosed(session: IoSession) = {
    val actor = actorFor(session)
    session.removeAttribute(ACTOR_KEY)
    actor ! MinaMessage.SessionClosed
  }
}
