package net.lag.kestrel.loadtest.thrift

import com.twitter.ostrich.stats.Stats
//import com.twitter.parrot.server.{ParrotRequest, ParrotService} // 0.4.6
import com.twitter.parrot.server.{ParrotRequest, ParrotThriftService} // 0.4.5
import java.nio.ByteBuffer
import net.lag.kestrel.loadtest.KestrelProducerLoadTestConfig
import net.lag.kestrel.thrift.Kestrel

// class KestrelThriftProducer(parrotService: ParrotService[ParrotRequest, Array[Byte]]) // 0.4.6
class KestrelThriftProducer(parrotService: ParrotThriftService)
extends AbstractKestrelThriftLoadTest[Int](parrotService) with KestrelProducerLoadTestConfig {
  Stats.incr("items_produced", 0)

  lazy val commands = {
    log.info("generating producer thrift commands")
    log.info(distribution.toString)

    distribution.map { segment =>
      (client: Kestrel.FinagledClient) => {
        client.put(segment.queueName, List(ByteBuffer.wrap(segment.payloadBytes))) onSuccess { n =>
          Stats.incr("items_produced", n)
        }
      }
    }
  }
}
