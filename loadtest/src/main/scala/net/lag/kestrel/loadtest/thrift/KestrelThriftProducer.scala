package net.lag.kestrel.loadtest.thrift

import com.twitter.ostrich.stats.Stats
import com.twitter.parrot.server.{ParrotService, ParrotRequest}
import java.nio.ByteBuffer
import net.lag.kestrel.loadtest.KestrelProducerLoadTestConfig
import net.lag.kestrel.thrift.Kestrel

class KestrelThriftProducer(parrotService: ParrotService[ParrotRequest, Array[Byte]])
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
