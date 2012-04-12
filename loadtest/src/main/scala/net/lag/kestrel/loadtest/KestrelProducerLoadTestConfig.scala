package net.lag.kestrel.loadtest

import com.twitter.conversions.storage._
import com.twitter.util.StorageUnit

sealed case class ProducerQueueDistributionSegment(val queueName: String,
                                                   val payloadSize: StorageUnit,
                                                   val frequency: Double = 1.0) {
  if (payloadSize > 100.megabytes) {
    throw new IllegalArgumentException("payload size is %s; are you insane?".format(payloadSize.toHuman))
  }

  lazy val payload = (0 until payloadSize.inBytes.toInt).foldLeft(new StringBuilder) {
    (s, _) => s.append("X")
  }.toString

  lazy val payloadBytes = payload.getBytes

  override def toString = "%s: %s @ %f%%".format(queueName, payloadSize.toHuman, frequency * 100.0)
}

sealed case class ProducerQueueDistribution(val segments: Seq[ProducerQueueDistributionSegment]) {
  import scala.math.round

  def map[T](f: (ProducerQueueDistributionSegment) => T): Seq[T] = {
    val minFrequency = segments.map { _.frequency }.min
    segments.flatMap { segment =>
      val repetitions = round(segment.frequency / minFrequency).toInt
      (1 to repetitions).foldLeft(Seq[T]()) { case (s, _) =>
        s :+ f(segment)
      }
    }
  }

  override def toString = "ProducerQueueDistribution: {\n\t%s\n}".format(segments.mkString("\n\t"))
}

object ProducerQueueDistribution {
  def simple(queueNameTemplate: String, numQueues: Int, payloadSize: StorageUnit) = {
    val segments = (0 until numQueues).map { q =>
      val queueName = queueNameTemplate.format(q)
      ProducerQueueDistributionSegment(queueName, payloadSize, 1.0 / numQueues.toDouble)
    }
    ProducerQueueDistribution(segments)
  }

  /**
   * Arbitrary distribution. ratesAndSizes contains a sequence of rates (in unspecified units) and payload
   * sizes. As long as rates are all in the same units (per second, per minute) they will be mapped to
   * the load test rate and will keep their relative magnitudes.
   */
  def arbitrary(queueNameTemplate: String, ratesAndSizes: Seq[(Int, StorageUnit)]) = {
    val totalRate = ratesAndSizes.foldLeft(0) { case (t, (rate, _)) => t + rate }.toDouble
    val multiplier = 1.0 / totalRate

    val segments = ratesAndSizes.foldLeft(Seq[ProducerQueueDistributionSegment]()) { case (seq, (rate, size)) =>
      val frequency = rate.toDouble * multiplier
      val queueName = queueNameTemplate.format(seq.size)
      seq ++ Seq(ProducerQueueDistributionSegment(queueName, size, frequency))
    }
    ProducerQueueDistribution(segments)
  }

  /**
   * Like arbitrary distribution, but all queues use the same sized payloads.
   */
  def arbitraryFixedSize(queueNameTemplate: String, rates: Seq[Int], payloadSize: StorageUnit) = {
    val ratesAndSizes = rates.map { r => (r, payloadSize) }
    arbitrary(queueNameTemplate, ratesAndSizes)
  }
}

trait KestrelProducerLoadTestConfig extends KestrelLoadTestConfig {
  var distribution = ProducerQueueDistribution.simple("queue_%d", 1, 128.bytes)
}
