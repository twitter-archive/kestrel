import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val defaultProject = "com.twitter" % "standard-project" % "0.1"
}
