import sbt._
import com.twitter.sbt._

class KestrelProject(info: ProjectInfo) extends StandardServiceProject(info) with NoisyDependencies
  with SubversionPublisher
  with DefaultRepos
  with IdeaProject
  with PublishSourcesAndJavadocs
  with PublishSite
{
  val util = "com.twitter" % "util-core" % "1.8.1"

  val ostrich = "com.twitter" % "ostrich" % "4.0.3"
  val naggati = "com.twitter" % "naggati" % "2.1.1"

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

  // 100 times: 10,000 items of 1024 bytes each.
//  override def fork = forkRun(List("-Xmx1024m", "-verbosegc", "-XX:+PrintGCDetails"))

  lazy val putMany = task { args =>
    runTask(Some("net.lag.kestrel.load.PutMany"), testClasspath, args).dependsOn(testCompile)
  } describedAs "Run a load test on PUT."

  lazy val manyClients = task { args =>
    runTask(Some("net.lag.kestrel.load.ManyClients"), testClasspath, args).dependsOn(testCompile)
  } describedAs "Run a load test on many slow clients."

  lazy val flood = task { args =>
    runTask(Some("net.lag.kestrel.load.Flood"), testClasspath, args).dependsOn(testCompile)
  } describedAs "Run a load test on a flood of PUT/GET."

  lazy val packing = task { args =>
    runTask(Some("net.lag.kestrel.load.JournalPacking"), testClasspath, args).dependsOn(testCompile)
  } describedAs "Run a load test on journal packing."
}
