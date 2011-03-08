import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val twitterMaven = "twitter.com" at "http://maven.twttr.com/"
  val standardProject = "com.twitter" % "standard-project" % "0.11.1"

  val lr = "less repo" at "http://repo.lessis.me"
  val gh = "me.lessis" % "sbt-gh-issues" % "0.0.1"

  val sbtIdeaRepo = "sbt-idea-repo" at "http://mpeltonen.github.com/maven/"
  val sbtIdea = "com.github.mpeltonen" % "sbt-idea-plugin" % "0.3.0"
}
