import sbt._
import com.twitter.sbt.StandardProject


class KestrelProject(info: ProjectInfo) extends StandardProject(info) {
  val specs = "org.scala-tools.testing" % "specs" % "1.6.2.1" // % "test"
  val xrayspecs = "com.twitter" % "xrayspecs" % "1.0.7" % "test"
  val vscaladoc = "org.scala-tools" % "vscaladoc" % "1.1-md-3"

  val configgy = "net.lag" % "configgy" % "1.5.2"
  val naggati = "net.lag" % "naggati" % "0.7.2"
  val mina = "org.apache.mina" % "mina-core" % "2.0.0-M6"
  val slf4j_api = "org.slf4j" % "slf4j-api" % "1.5.2"
  val slf4j_jdk14 = "org.slf4j" % "slf4j-jdk14" % "1.5.2"
}
