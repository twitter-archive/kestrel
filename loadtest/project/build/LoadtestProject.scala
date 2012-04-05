import sbt._
import com.twitter.sbt._
import java.io.File

class LoadtestProject(info: ProjectInfo) extends StandardServiceProject(info)
  with ProjectDependencies
  with DefaultRepos
  with CompileThriftRuby
  with CompileThriftScroogeMixin
{
  val twitterPrivate = "twitter.com" at "http://binaries.local.twitter.com/maven/"

  override def filterScalaJars = false

  val parrot = "com.twitter" % "parrot" % "0.4.6-SNAPSHOT"
  val scrooge_runtime = "com.twitter" %% "scrooge-runtime" % "1.1.3"
  override def scroogeVersion = "1.1.7"

  override def ivyXML =
    <dependencies>
      <exclude org="com.sun.jdmk"/>
      <exclude org="com.sun.jmx"/>
      <exclude org="javax.jms"/>
    </dependencies>

  override def mainClass = Some("com.twitter.parrot.launcher.LauncherMain")

  lazy val startMemcacheConsumer = startParrotTask("memcache", "consumer")
  lazy val startMemcacheProducer = startParrotTask("memcache", "producer")
  lazy val killMemcacheConsumer = killParrotTask("memcache", "consumer")
  lazy val killMemcacheProducer = killParrotTask("memcache", "producer")

  lazy val startThriftConsumer = startParrotTask("thrift", "consumer")
  lazy val startThriftProducer = startParrotTask("thrift", "producer")
  lazy val killThriftConsumer = killParrotTask("thrift", "consumer")
  lazy val killThriftProducer = killParrotTask("thrift", "producer")

  def makeConfigPath(protocol: String, jobType: String) = "config/kestrel-%s-%s.scala".format(protocol, jobType)

  def startParrotTask(protocol: String, jobType: String) = {
    val config = makeConfigPath(protocol, jobType)
    val task =
      if (new File(config).exists) {
        runParrotTask(Array("-f", config))
      } else {
        errorTask("file not found: %s".format(config))
      }
    task describedAs("start %s protocol %s".format(protocol, jobType))
  }

  def killParrotTask(protocol: String, jobType: String) = {
    val config = makeConfigPath(protocol, jobType)
    val task =
      if (new File(config).exists) {
        runParrotTask(Array("-f", config, "-k"))
      } else {
        errorTask("file not found: %s".format(config))
      }
    task describedAs("kill %s protocol %s".format(protocol, jobType))
  }

  def runParrotTask(args: Array[String]) =
    runTask(mainClass, runClasspath, args) dependsOn(compile, copyResources)
}
