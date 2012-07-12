import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.logging.config._
import com.twitter.ostrich.admin.config._
import net.lag.kestrel.config._

new KestrelConfig {
  listenAddress = "0.0.0.0"
  memcacheListenPort = 22133
  textListenPort = 2222
  thriftListenPort = 2229

  queuePath = "/var/spool/kestrel"

  clientTimeout = 30.seconds

  expirationTimerFrequency = 1.second

  maxOpenTransactions = 100

  // default queue settings:
  default.defaultJournalSize = 16.megabytes
  default.maxMemorySize = 128.megabytes
  default.maxJournalSize = 1.gigabyte

  admin.httpPort = 2223

  admin.statsNodes = new StatsConfig {
    reporters = new TimeSeriesCollectorConfig
  }

  queues = new QueueBuilder {
    // keep items for no longer than a half hour, and don't accept any more if
    // the queue reaches 1.5M items.
    name = "weather_updates"
    maxAge = 1800.seconds
    maxItems = 1500000
  } :: new QueueBuilder {
    // don't keep a journal file for this queue. when kestrel exits, any
    // remaining contents will be lost.
    name = "transient_events"
    keepJournal = false
  } :: new QueueBuilder {
    name = "jobs_pending"
    expireToQueue = "jobs_ready"
    maxAge = 30.seconds
  } :: new QueueBuilder {
    name = "jobs_ready"
    syncJournal = 0.seconds
  } :: new QueueBuilder {
    name = "spam"
  } :: new QueueBuilder {
    name = "spam0"
  } :: new QueueBuilder {
    name = "hello"
    fanoutOnly = true
  } :: new QueueBuilder {
    name = "small"
    maxSize = 128.megabytes
    maxMemorySize = 16.megabytes
    maxJournalSize = 128.megabytes
    discardOldWhenFull = true
  } :: new QueueBuilder {
    name = "slow"
    syncJournal = 10.milliseconds
  }

  aliases = new AliasBuilder {
    name = "wx_updates"
    destinationQueues = List("weather_updates")
  } :: new AliasBuilder {
    name = "spam_all"
    destinationQueues = List("spam", "spam0")
  }

  loggers = new LoggerConfig {
    level = Level.INFO
    handlers = new FileHandlerConfig {
      filename = "/var/log/kestrel/kestrel.log"
      roll = Policy.Never
    }
  }
}
