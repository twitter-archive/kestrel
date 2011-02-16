import com.twitter.admin.config._
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.logging.config._
import net.lag.kestrel.config._

new KestrelConfig {
  listenAddress = "0.0.0.0"
  memcacheListenPort = 22133
  textListenPort = 2222

  queuePath = "/var/spool/kestrel"

  clientTimeout = 30.seconds

  expirationTimerFrequency = 1.second

  maxOpenTransactions = 100

  // default queue settings:
  default.maxJournalSize = 16.megabytes
  default.maxMemorySize = 128.megabytes
  default.maxJournalOverflow = 10

  admin.httpPort = 2223

  admin.statsNodes = new StatsConfig {
    reporters = new JsonStatsLoggerConfig {
      loggerName = "stats"
      serviceName = "kestrel"
    } :: new TimeSeriesCollectorConfig
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
    syncJournal = true
  } :: new QueueBuilder {
    name = "spam"
    multifileJournal = true
  } :: new QueueBuilder {
    name = "spam0"
    multifileJournal = true
  } :: new QueueBuilder {
    name = "hello"
    fanoutOnly = true
  }

  loggers = new LoggerConfig {
    level = Level.INFO
    handlers = new FileHandlerConfig {
      filename = "/var/log/kestrel/kestrel.log"
      roll = Policy.Never
    }
  } :: new LoggerConfig {
    node = "stats"
    level = Level.INFO
    useParents = false
    handlers = new FileHandlerConfig {
      filename = "/var/log/kestrel/stats.log"
      roll = Policy.Never
    }
  }
}
