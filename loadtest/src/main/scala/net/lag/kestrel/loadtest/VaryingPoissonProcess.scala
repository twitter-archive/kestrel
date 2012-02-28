package net.lag.kestrel.loadtest

import util.Random
import com.twitter.conversions.time._
import com.twitter.parrot.util.RequestDistribution
import com.twitter.util.{Duration, JavaTimer, Time}
import org.apache.commons.math.distribution.ExponentialDistributionImpl
import java.util.concurrent.TimeUnit
import java.lang.IllegalArgumentException

case class ArrivalRate(val arrivalsPerSecond: Int, val duration: Duration) {
  if (arrivalsPerSecond <= 0) {
    throw new IllegalArgumentException("arrivals must be >= 1")
  }

  if (duration < 1.millisecond) {
    throw new IllegalArgumentException("duration must be >= 1 millisecond")
  }
}

class VaryingPoissonProcess(val arrivalRates: Seq[ArrivalRate]) extends RequestDistribution {
  if (arrivalRates.isEmpty) {
    throw new IllegalArgumentException("must have at least one arrival rate")
  }

  private[this] val rand = new Random(Time.now.inMillis)
  private[this] val timer = new JavaTimer(true)

  private[this] var iter: Iterator[ArrivalRate] = Iterator.empty
  private[this] var dist: Option[ExponentialDistributionImpl] = None

  private def updateDistribution() {
    synchronized {
      if (!iter.hasNext) {
        iter = arrivalRates.iterator
      }

      val nextArrivalRate = iter.next
      dist = Some(new ExponentialDistributionImpl(1000000000.0 / nextArrivalRate.arrivalsPerSecond))
      timer.schedule(Time.now + nextArrivalRate.duration) {
        updateDistribution()
      }
    }
  }

  def timeToNextArrival(): Duration = {
    if (!dist.isDefined) {
      updateDistribution()
    }

    val nanosToNextArrival = dist.get.inverseCumulativeProbability(rand.nextDouble())
    Duration(nanosToNextArrival.toLong, TimeUnit.NANOSECONDS)
  }
}
