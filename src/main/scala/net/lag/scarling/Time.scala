package net.lag.scarling

// stolen from nick's ruby version.
// this lets unit tests muck around with temporality without calling Thread.sleep().
object Time {
  private var offset: Long = 0

  final def now() = System.currentTimeMillis + offset
  final def advance(msec: Int) = offset += msec
}
