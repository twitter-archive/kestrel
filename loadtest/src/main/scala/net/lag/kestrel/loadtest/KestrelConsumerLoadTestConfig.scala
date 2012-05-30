package net.lag.kestrel.loadtest

sealed case class ConsumerQueueDistributionSegment(val queueName: String,
                                                   val frequency: Double = 1.0) {
  override def toString = "%s: @ %f%%".format(queueName, frequency * 100.0)
}

sealed case class ConsumerQueueDistribution(val segments: Seq[ConsumerQueueDistributionSegment]) {
  import scala.math.round

  def map[T](f: (ConsumerQueueDistributionSegment) => T): Seq[T] = {
    val minFrequency = segments.map { _.frequency }.min
    segments.flatMap { segment =>
      val repetitions = round(segment.frequency / minFrequency).toInt
      (1 to repetitions).foldLeft(Seq[T]()) { case (s, _) =>
        s :+ f(segment)
      }
    }
  }

  override def toString = "ConsumerQueueDistribution: {\n\t%s\n}".format(segments.mkString("\n\t"))
}

object ConsumerQueueDistribution {
  def simple(queueNameTemplate: String, numQueues: Int, numFanouts: Int) = {
    val totalQueues = numQueues * (numFanouts + 1)

    val segments = (0 until numQueues).flatMap { q =>
      (-1 until numFanouts).map { f =>
        ConsumerQueueDistributionSegment(queueName(queueNameTemplate, q, f), 1.0 / totalQueues.toDouble)
      }
    }

    ConsumerQueueDistribution(segments)
  }

  def simpleWithoutMasterReads(queueNameTemplate: String, numQueues: Int, numFanouts: Int) = {
    val totalQueues = numQueues * numFanouts

    val segments = (0 until numQueues).flatMap { q =>
      (0 until numFanouts).map { f =>
        ConsumerQueueDistributionSegment(queueName(queueNameTemplate, q, f), 1.0 / totalQueues.toDouble)
      }
    }
    ConsumerQueueDistribution(segments)
  }

  /**
   * Arbitrary distribution. ratesAndFanouts contains a sequence of rates (in unspecified units) and
   * number of fanouts. As long as rates are all in the same units (per second, per minute) they
   * will be mapped to the load test rate and will keep their relative magnitudes. Rate for each fanout
   * is equal.
   */
  def arbitrary(queueNameTemplate: String, ratesAndFanouts: Seq[(Int, Int)], performMasterReads: Boolean = true) = {
    val clampedRatesAndFanouts = ratesAndFanouts.map { case (rate, fanouts) =>
      (rate max 1, fanouts max 0)
    }
    val totalRate = clampedRatesAndFanouts.foldLeft(0) { case (t, (rate, fanouts)) =>
      val numReads =
        if (fanouts > 0) {
          if (performMasterReads) {
            fanouts + 1
          } else {
            fanouts
          }
        } else {
          1
        }
      t + (rate * numReads)
    }.toDouble
    val multiplier = 1.0 / totalRate

    val indexed = clampedRatesAndFanouts.zipWithIndex
    val segments =
      indexed.foldLeft(Seq[ConsumerQueueDistributionSegment]()) { case (seq, ((rate, fanouts), index)) =>
        val start =
          if (fanouts > 0) {
            if (performMasterReads) -1 else 0
          } else {
            -1
          }

        val fanoutSegments = (start until fanouts).map { fanout =>
          val frequency = rate.toDouble * multiplier
          val name = queueName(queueNameTemplate, index, fanout)
          ConsumerQueueDistributionSegment(name, frequency)
        }
        seq ++ fanoutSegments
      }
    ConsumerQueueDistribution(segments)
  }

  /**
   * Arbitrary distribution without fanouts. rates contains a sequence of rates (in unspecified units).
   * As long as rates are all in the same units (per second, per minute) they will be mapped to the load
   * test rate and will keep their relative magnitudes.
   */
  def arbitraryNoFanouts(queueNameTemplate: String, rates: Seq[Int]) = {
    arbitrary(queueNameTemplate, rates.map { r => (r, 0) })
  }

  private[this] def queueName(queueNameTemplate: String, queueNum: Int, fanoutNum: Int) = {
    val base = queueNameTemplate.format(queueNum)
    if (fanoutNum >= 0) {
      base + "+%d".format(fanoutNum)
    } else {
      base
    }
  }
}

trait KestrelConsumerLoadTestConfig extends KestrelLoadTestConfig {
  var distribution = ConsumerQueueDistribution.simple("queue_%d", 1, 0)
}
