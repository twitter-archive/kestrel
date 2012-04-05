package net.lag.kestrel.loadtest

import com.twitter.logging.Logger
import com.twitter.parrot.processor.RecordProcessor
import com.twitter.parrot.config.CommonParserConfig

trait KestrelRecordProcessor extends RecordProcessor {
  val rng = {
    val config = new CommonParserConfig { }
    config.randomizer
  }

  def shuffle[T](list: Seq[T]): Seq[T] = {
    list
      .map { t => (rng.nextInt, t) }
      .sortBy { case (n, _) => n }
      .map { case (_, t) => t }
  }


  val log = Logger.get("kestrel-loadtest")
}

trait KestrelLoadTestConfig {
  var numQueues = 1
  var timeout = 0
  var queueNameTemplate = "queue_%d"
}

trait KestrelProducerLoadTestConfig extends KestrelLoadTestConfig {
  var payloadSize = 128

  lazy val queueNames = {
    (0 until numQueues).map { q =>
      queueNameTemplate.format(q)
    }
  }

  lazy val payload = (0 until payloadSize).foldLeft(new StringBuilder) { (s, _) => s.append("X") }.toString
}

trait KestrelConsumerLoadTestConfig extends KestrelLoadTestConfig {
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

}
