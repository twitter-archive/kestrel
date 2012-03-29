import sbt._
import com.twitter.sbt._

class LoadtestProject(info: ProjectInfo) extends StandardServiceProject(info)
  with ProjectDependencies
  with DefaultRepos
{
  val twitterPrivate = "twitter.com" at "http://binaries.local.twitter.com/maven/"

  override def filterScalaJars = false

  override def mainClass = Some("com.twitter.parrot.launcher.LauncherMain")

  lazy val startParrotConsumer = runParrot("config/kestrel-dataservices-consumer.scala")
  lazy val killParrotConsumer = killParrot("config/kestrel-dataservices-consumer.scala")

  lazy val startParrotProducer = runParrot("config/kestrel-dataservices-producer.scala")
  lazy val killParrotProducer = killParrot("config/kestrel-dataservices-producer.scala")

  def runParrotTask(args: Array[String]) = task { _ =>
    runTask(mainClass, runClasspath, args) dependsOn(compile, copyResources)
  }

  def runParrot(config: String) = runParrotTask(Array("-f", config))
  def killParrot(config: String) = runParrotTask(Array("-f", config, "-k"))

  val parrot = "com.twitter" % "parrot" % "0.4.5"

  override def ivyXML =
    <dependencies>
      <exclude org="com.sun.jdmk"/>
      <exclude org="com.sun.jmx"/>
      <exclude org="javax.jms"/>
    </dependencies>
}
