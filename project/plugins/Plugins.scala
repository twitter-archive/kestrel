import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val scalaTools = "nexus" at "http://nexus.scala-tools.org/content/repositories/releases/"
  val defaultProject = "com.twitter" % "standard-project" % "0.7.1"
}
