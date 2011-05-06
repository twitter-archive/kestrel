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
  override def ivyRepositories = Seq(Resolver.defaultLocal(None)) ++ repositories ++
    Set("scala-tools" at "http://scala-tools.org/repo-releases/",
        "freemarker" at "http://freemarker.sourceforge.net/maven2/")

  val standardProject = "com.twitter" % "standard-project" % "0.12.3"

  val sbtIdeaRepo = "sbt-idea-repo" at "http://mpeltonen.github.com/maven/"
  val sbtIdea = "com.github.mpeltonen" % "sbt-idea-plugin" % "0.4.0"
}

