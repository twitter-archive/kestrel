package net.lag.kestrel.loadtest

trait KestrelLoadTestConfig {
  var numQueues = 1
  var timeout = 0
  var queueNameTemplate = "queue_%d"
}
