import sbt._
import com.twitter.sbt._

class KestrelProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher with InlineDependencies {
  inline("net.lag" % "configgy" % "1.6.4")
  val naggati = "net.lag" %% "naggati" % "0.7.4"
  val twitterActors = "com.twitter" % "twitteractors" % "1.1.0"
  val mina = "org.apache.mina" % "mina-core" % "2.0.0-M6"
  val slf4j_api = "org.slf4j" % "slf4j-api" % "1.5.2"
  val slf4j_jdk14 = "org.slf4j" % "slf4j-jdk14" % "1.5.2"
  val xrayspecs = "com.twitter" % "xrayspecs" % "1.0.7"

  val specs = "org.scala-tools.testing" % "specs" % "1.6.2.1" % "test"

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
