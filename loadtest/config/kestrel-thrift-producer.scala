import com.twitter.parrot.config.ParrotLauncherConfig
import java.io._

new ParrotLauncherConfig {
  mesosCluster = "smfd-devel"
  hadoopNS = "hdfs://hadoop-scribe-nn.smfd.twitter.com"
  hadoopConfig = "/etc/hadoop/hadoop-conf-smfd"

  zkHostName = Some("zookeeper.smfd.twitter.com")

  distDir = "dist/kestrel_loadtest"
  jobName = "kestrel_thrift_producer"
  port = 2229
  victims = "smfd-akc-04-sr1.devel.twitter.com"
  parser = "thrift" // magic

  hostConnectionLimit = 5000

  log = {
    val file = File.createTempFile("kestrel", "parrot")
    val writer = new FileWriter(file)
    (1 to 50000).foreach { x => writer.write("dummy %d\n".format(x)) }
    writer.close
    file.getAbsolutePath
  }
  requestRate = 1250
  numInstances = 1
  duration = 60
  timeUnit = "MINUTES"

  imports = """import net.lag.kestrel.loadtest.thrift.KestrelThriftProducer
               import net.lag.kestrel.loadtest._
               import com.twitter.parrot.util.SlowStartPoissonProcess
               import com.twitter.conversions.time._
               import com.twitter.conversions.storage._"""

  createDistribution = "createDistribution = { rate => new SlowStartPoissonProcess(rate, 5.minutes) }"

  responseType = "Array[Byte]"
  transport = "ThriftTransport"
  loadTest = """new KestrelThriftProducer(service.get) {
                  distribution = ProducerQueueDistribution.simple("vshard_%d", 10, 50.bytes)
                }"""

  doConfirm = false
}
