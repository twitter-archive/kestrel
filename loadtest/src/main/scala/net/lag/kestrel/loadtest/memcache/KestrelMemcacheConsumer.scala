package net.lag.kestrel.loadtest.memcache

import com.twitter.finagle.kestrel.protocol.{Response => KestrelResponse}
import com.twitter.parrot.server.{ParrotService, ParrotRequest}
import net.lag.kestrel.loadtest.KestrelConsumerLoadTestConfig

class KestrelMemcacheConsumer(service: ParrotService[ParrotRequest, KestrelResponse])
extends AbstractKestrelMemcacheLoadTest(service) with KestrelConsumerLoadTestConfig {
  val statName = "items_consumed"

  lazy val commands = {
    log.info("generating consumer memcache commands")
    log.info(distribution.toString)

    distribution.map { segment =>
      if (timeout > 0) {
        "get %s/t=%d".format(segment.queueName, timeout)
      } else {
        "get %s".format(segment.queueName)
      }
    }
  }
}
