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
    log.info("generating producer commands from %d queue names", queueNames.size)
    log.info("numQueues: %d, queueNameTemplate: '%s', payloadSize: %d",
	numQueues, queueNameTemplate, payloadSize)
    log.info("queueNames: %s", queueNames.mkString(", "))

    val payloadBytes = payload.getBytes

    queueNames.map { queueName =>
      (client: Kestrel.FinagledClient) => {
        client.put(queueName, List(ByteBuffer.wrap(payloadBytes))) onSuccess { n =>
          Stats.incr("items_produced", n)
        }
      }
    }
  }
}
