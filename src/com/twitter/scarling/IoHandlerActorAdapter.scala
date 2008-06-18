package com.twitter.scarling

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
            case null => {
                // weird bad: an exception happened but i guess it wasn't associated with any existing session.
                log.error(cause, "Exception inside mina!")
            }
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
