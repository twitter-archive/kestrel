import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  import scala.collection.jcl
  val environment = jcl.Map(System.getenv())
  def isSBTOpenTwitter = environment.get("SBT_OPEN_TWITTER").isDefined
  def isSBTTwitter = environment.get("SBT_TWITTER").isDefined

  override def repositories = if (isSBTOpenTwitter) {
    Set("twitter.artifactory" at "http://artifactory.local.twitter.com/open-source/")
  } else if (isSBTTwitter) {
    Set("twitter.artifactory" at "http://artifactory.local.twitter.com/repo/")
  } else {
    super.repositories ++ Seq("twitter.com" at "http://maven.twttr.com/")
  }
  override def ivyRepositories = Seq(Resolver.defaultLocal(None)) ++ repositories

  val standardProject = "com.twitter" % "standard-project" % "0.11.7"

  val lr = "less repo" at "http://repo.lessis.me"
  val gh = "me.lessis" % "sbt-gh-issues" % "0.0.1"

  val sbtIdeaRepo = "sbt-idea-repo" at "http://mpeltonen.github.com/maven/"
  val sbtIdea = "com.github.mpeltonen" % "sbt-idea-plugin" % "0.3.0"
}

