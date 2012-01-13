import com.twitter.parrot.config.ParrotLauncherConfig
import java.io._

new ParrotLauncherConfig {
  mesosCluster = "smfd-devel"
  hadoopNS = "hdfs://hadoop-scribe-nn.smfd.twitter.com"
  zkHostName = Some("zookeeper.smfd.twitter.com")

  distDir = "dist/kestrel_loadtest"
  jobName = "kestrel_consumer"
  port = 22133
  victims = "smfd-akc-04-sr1.devel.twitter.com"
  parser = "thrift" // magic

  hostConnectionLimit = 2000

  log = {
    val file = File.createTempFile("kestrel", "parrot")
    val writer = new FileWriter(file)
    (1 to 50000).foreach { x => writer.write("dummy %d\n".format(x)) }
    writer.close
    file.getAbsolutePath
  }
  requestRate = 100
  numInstances = 1
  duration = 10
  timeUnit = "MINUTES"

  imports = """import net.lag.kestrel.loadtest.KestrelConsumerLoadTest
               import com.twitter.finagle.kestrel.protocol.Response"""
  responseType = "Response"
  transport = "KestrelTransport"
  loadTest = """new KestrelConsumerLoadTest(kestrelService) {
                  numQueues = 10
                  numFanouts = 5
                  timeout = 100
                  queueNameTemplate = "vshard_%d"
                }"""

   doConfirm = false
}
