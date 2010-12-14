import sbt._
import com.twitter.sbt._

class KestrelProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher {
  val configgy = "com.twitter" % "configgy" % "3.0.0-SNAPSHOT"
  val ostrich = "com.twitter" % "ostrich" % "3.0.0-SNAPSHOT"
  val naggati = "com.twitter" % "naggati" % "2.0.0-SNAPSHOT"

  val specs = "org.scala-tools.testing" %% "specs" % "1.6.5" % "test"

  // workaround bug in sbt that hides scala-compiler.
  override def filterScalaJars = false
  val what = "org.scala-lang" % "scala-compiler" % "2.8.1"

  override def mainClass = Some("net.lag.kestrel.Kestrel")

  override def pomExtra =
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>

  override def releaseBuild = true

  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public")

  // 100 times: 10,000 items of 1024 bytes each.
  override def fork = forkRun
  lazy val putMany = runTask(Some("net.lag.kestrel.load.PutMany"), testClasspath, "100", "10000", "1024").dependsOn(testCompile) describedAs "Run a load test."
  lazy val manyClients = runTask(Some("net.lag.kestrel.load.ManyClients"), testClasspath).dependsOn(testCompile)
  lazy val flood = task { args =>
    runTask(Some("net.lag.kestrel.load.Flood"), testClasspath, args).dependsOn(testCompile)
  }
}
