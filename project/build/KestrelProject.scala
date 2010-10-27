import sbt._
import com.twitter.sbt._

class KestrelProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher {
  val configgy = "net.lag" % "configgy" % "2.0.1"
  val naggati = "com.twitter" %% "naggati" % "2.0.0-SNAPSHOT"
  val xrayspecs = "com.twitter" %% "xrayspecs" % "2.0"

  val specs = "org.scala-tools.testing" %% "specs" % "1.6.5" % "test"

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
}
