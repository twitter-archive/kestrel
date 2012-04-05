package net.lag.kestrel.loadtest.memcache

import com.twitter.finagle.kestrel.protocol.{Response => KestrelResponse}
import com.twitter.parrot.server.{ParrotService, ParrotRequest}
import net.lag.kestrel.loadtest.KestrelProducerLoadTestConfig

class KestrelMemcacheProducer(service: ParrotService[ParrotRequest, KestrelResponse])
extends AbstractKestrelMemcacheLoadTest(service) with KestrelProducerLoadTestConfig {
  val statName = "items_produced"

  lazy val commands = {
    log.info("generating producer commands from %d queue names", queueNames.size)
    log.info("numQueues: %d, queueNameTemplate: '%s', payloadSize: %d",
	numQueues, queueNameTemplate, payloadSize)
    log.info("queueNames: %s", queueNames.mkString(", "))

    queueNames.map { queueName =>
      "set %s 0 0 %d\r\n%s".format(queueName, payloadSize, payload)
    }
  }
}
