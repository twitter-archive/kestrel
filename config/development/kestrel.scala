import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import net.lag.kestrel.config._

new KestrelConfig {
  listenAddress = "0.0.0.0"
  memcacheListenPort = 22133

  queuePath = "/var/spool/kestrel"

  clientTimeout = 30.seconds

  expirationTimerFrequency = 1.second

  maxOpenTransactions = 100

  // default queue settings:
  default.maxJournalSize = 16.megabytes
  default.maxMemorySize = 128.megabytes
  default.maxJournalOverflow = 10

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
  }
}
