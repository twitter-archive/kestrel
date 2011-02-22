import sbt._
import com.twitter.sbt._

class KestrelProject(info: ProjectInfo) extends StandardServiceProject(info)
with SubversionPublisher with DefaultRepos with gh.Issues {
  val util = "com.twitter" % "util" % "1.6.6"

  val ostrich = "com.twitter" % "ostrich" % "3.0.4"
  val naggati = "com.twitter" % "naggati" % "2.0.0"
  val finagle = "com.twitter" % "finagle-core" % "1.1.25-SNAPSHOT"

  val specs = "org.scala-tools.testing" % "specs_2.8.1" % "1.6.7" % "test"
  val jmock = "org.jmock" % "jmock" % "2.4.0" % "test"
  val cglib = "cglib" % "cglib" % "2.1_3" % "test"
  val asm = "asm" % "asm" % "1.5.3" % "test"
  val objenesis = "org.objenesis" % "objenesis" % "1.1" % "test"
  val hamcrest = "org.hamcrest" % "hamcrest-all" % "1.1" % "test"


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

  def ghCredentials = gh.LocalGhCreds(log)
  def ghRepository = ("robey", "kestrel")

  // 100 times: 10,000 items of 1024 bytes each.
  override def fork = forkRun
  lazy val putMany = runTask(Some("net.lag.kestrel.load.PutMany"), testClasspath, "100", "10000", "1024").dependsOn(testCompile) describedAs "Run a load test."
  lazy val manyClients = runTask(Some("net.lag.kestrel.load.ManyClients"), testClasspath).dependsOn(testCompile)

  lazy val flood = task { args =>
    runTask(Some("net.lag.kestrel.load.Flood"), testClasspath, args).dependsOn(testCompile)
  }

  lazy val packing = task { args =>
    runTask(Some("net.lag.kestrel.load.JournalPacking"), testClasspath, args).dependsOn(testCompile)
  }
}
