package net.lag.kestrel.loadtest.memcache

import com.twitter.finagle.kestrel.protocol.{Response => KestrelResponse, Error => KestrelError}
import com.twitter.ostrich.stats.Stats
//import com.twitter.parrot.server.{ParrotService, ParrotRequest}       // 0.4.6
import com.twitter.parrot.server.{ParrotKestrelService, ParrotRequest} // 0.4.5
import com.twitter.parrot.thrift.ParrotJob
import net.lag.kestrel.loadtest.KestrelRecordProcessor

//abstract class AbstractKestrelMemcacheLoadTest(service: ParrotService[ParrotRequest, KestrelResponse]) // 0.4.6
abstract class AbstractKestrelMemcacheLoadTest(service: ParrotKestrelService)
extends KestrelRecordProcessor {
  def commands: Seq[String]

  def statName: String

  def start() {
    Stats.incr(statName, 0)
  }

  def processLines(job: ParrotJob, lines: Seq[String]) {
    val numVictims = job.victims.size

    log.debug("process %d lines", lines.size)

    shuffle(commands).foreach { command =>
      val target = job.victims.get(rng.nextInt(numVictims))

      log.debug("create request from '%s' for '%s'", command, target)
      try {
        service(new ParrotRequest(target, None, Nil, null, command)) onSuccess {
          case KestrelError() => log.error("unspecified (client) error")
          case _ =>              Stats.incr(statName)
        } onFailure { t =>
          log.error(t, "request error")
        }
      } catch {
        case ex => log.error(ex, "request generation error")
      }
    }
  }
}
