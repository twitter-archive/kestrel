package net.lag.kestrel.loadtest.memcache

import com.twitter.finagle.kestrel.protocol.{Response => KestrelResponse}
//import com.twitter.parrot.server.{ParrotService, ParrotRequest}       // 0.4.6
import com.twitter.parrot.server.{ParrotKestrelService, ParrotRequest} // 0.4.5
import net.lag.kestrel.loadtest.KestrelProducerLoadTestConfig

//class KestrelMemcacheProducer(service: ParrotService[ParrotRequest, KestrelResponse]) // 0.4.6
class KestrelMemcacheProducer(service: ParrotKestrelService)
extends AbstractKestrelMemcacheLoadTest(service) with KestrelProducerLoadTestConfig {
  val statName = "items_produced"

  lazy val commands = {
    log.info("generating producer memcache commands")
    log.info(distribution.toString)

    distribution.map { segment =>
      "set %s 0 0 %d\r\n%s".format(segment.queueName, segment.payloadSize.inBytes, segment.payload)
    }
  }
}
