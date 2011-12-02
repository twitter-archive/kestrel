package net.lag.kestrel.loadtest

import com.twitter.parrot.server.ParrotKestrelService

class KestrelProducerLoadTest(service: ParrotKestrelService) extends AbstractKestrelLoadTest(service) {
  var payloadSize = 128

  lazy val queueNames = {
    (0 until numQueues).map { q =>
      queueNameTemplate.format(q)
    }
  }

  lazy val payload = (0 until payloadSize).foldLeft(new StringBuilder) { (s, _) => s.append("X") }.toString

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
