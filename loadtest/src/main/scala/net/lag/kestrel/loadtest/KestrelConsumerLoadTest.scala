package net.lag.kestrel.loadtest

import com.twitter.parrot.server.ParrotKestrelService

class KestrelConsumerLoadTest(service: ParrotKestrelService) extends AbstractKestrelLoadTest(service) {
  var numFanouts = 0
  var readFromMasterQueue = true // if false, does not read from master queue

  lazy val queueNames = {
    val fanoutQueueNames = if (numFanouts > 0) {
      val fanoutQueueNameTemplate = queueNameTemplate + "+%d"
      (0 until numQueues).flatMap { q =>
        (0 until numFanouts).map { f =>
          fanoutQueueNameTemplate.format(q, f)
        }
      }
    } else {
      Seq[String]()
    }

    val masterQueueNames = if (readFromMasterQueue) {
      (0 until numQueues).map { q =>
        queueNameTemplate.format(q)
      }
    } else {
      Seq[String]()
    }

    masterQueueNames ++ fanoutQueueNames
  }

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
