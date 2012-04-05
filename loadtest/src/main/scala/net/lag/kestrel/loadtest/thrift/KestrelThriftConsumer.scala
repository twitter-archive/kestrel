package net.lag.kestrel.loadtest.thrift

import com.twitter.ostrich.stats.Stats
import com.twitter.parrot.server.{ParrotService, ParrotRequest}
import java.nio.ByteBuffer
import net.lag.kestrel.loadtest.KestrelConsumerLoadTestConfig
import net.lag.kestrel.thrift.{Item, Kestrel}

class KestrelThriftConsumer(parrotService: ParrotService[ParrotRequest, Array[Byte]])
extends AbstractKestrelThriftLoadTest[Seq[Item]](parrotService) with KestrelConsumerLoadTestConfig {
  var maxItemsPerRequest = 1

  Stats.incr("items_consumed", 0)

  lazy val commands = {
    log.info("generating consumer commands from %d queue names", queueNames.size)
    log.info("numQueues: %d, numFanouts: %d, queueNameTemplate: '%s'",
	numQueues, numFanouts, queueNameTemplate)
    log.info("queueNames: %s", queueNames.mkString(", "))

    queueNames.map { queueName =>
      if (timeout > 0) {
        (client: Kestrel.FinagledClient) => {
          client.get(queueName, maxItemsPerRequest, timeout) onSuccess { items =>
            Stats.incr("items_consumed", items.size)
          }
        }
      } else {
        (client: Kestrel.FinagledClient) => {
          client.get(queueName, maxItemsPerRequest) onSuccess { items =>
            Stats.incr("items_consumed", items.size)
          }
        }
      }
    }
  }
}
