package net.lag.kestrel.loadtest.memcache

import com.twitter.finagle.kestrel.protocol.{Response => KestrelResponse}
import com.twitter.parrot.server.{ParrotService, ParrotRequest}
import net.lag.kestrel.loadtest.KestrelConsumerLoadTestConfig

class KestrelMemcacheConsumer(service: ParrotService[ParrotRequest, KestrelResponse])
extends AbstractKestrelMemcacheLoadTest(service) with KestrelConsumerLoadTestConfig {
  val statName = "items_consumed"

  lazy val commands = {
    log.info("generating consumer commands from %d queue names", queueNames.size)
    log.info("numQueues: %d, numFanouts: %d, queueNameTemplate: '%s'",
	numQueues, numFanouts, queueNameTemplate)
    log.info("queueNames: %s", queueNames.mkString(", "))

    queueNames.map { queueName =>
      if (timeout > 0) {
        "get %s/t=%d".format(queueName, timeout)
      } else {
        "get %s".format(queueName)
      }
    }
  }
}
