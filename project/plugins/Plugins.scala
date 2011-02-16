import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val twitterMaven = "twitter.com" at "http://maven.twttr.com/"
  val defaultProject = "com.twitter" % "standard-project" % "0.9.17"

  val lr = "less repo" at "http://repo.lessis.me"
  val gh = "me.lessis" % "sbt-gh-issues" % "0.0.1"
}
