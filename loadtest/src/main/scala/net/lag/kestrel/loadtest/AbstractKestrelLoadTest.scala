package net.lag.kestrel.loadtest

import com.twitter.logging.Logger
import com.twitter.parrot.processor.RecordProcessor
import com.twitter.parrot.config.CommonParserConfig
import com.twitter.parrot.server.{ParrotKestrelService, ParrotRequest}
import com.twitter.parrot.thrift.ParrotJob

abstract class AbstractKestrelLoadTest(service: ParrotKestrelService) extends RecordProcessor {
  var numQueues = 1
  var timeout = 0
  var queueNameTemplate = "queue_%d"

  val log = Logger.get("kestrel-loadtest")

  val rng = {
    val config = new CommonParserConfig { }
    config.randomizer
  }

  def shuffle[T](list: Seq[T]): Seq[T] = {
    list
      .map { t => (rng.nextInt, t) }
      .sortBy { case (n, _) => n }
      .map { case (_, t) => t }
  }

  def commands: Seq[String]

  def processLines(job: ParrotJob, lines: Seq[String]) {
    val numVictims = job.victims.size

    log.debug("process %d lines", lines.size)

    shuffle(commands).map { command =>
      val target = job.victims.get(rng.nextInt(numVictims))

      log.debug("create request from '%s' for '%s'", command, target)
      try {
        Some(service(new ParrotRequest(target, None, Nil, null, command)))
      } catch {
        case ex => log.error(ex, "request error")
      }
    }
  }
}
