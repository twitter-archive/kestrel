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
