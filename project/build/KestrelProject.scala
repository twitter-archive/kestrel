import sbt._
import com.twitter.sbt._

class KestrelProject(info: ProjectInfo)
  extends StandardServiceProject(info)
  with NoisyDependencies
  with DefaultRepos
  with CompileThriftScrooge
  with SubversionPublisher
  with PublishSite
{
  val finagleVersion = "1.9.6"

  val libkestrel = "com.twitter" % "libkestrel" % "1.0.0-SNAPSHOT"
  val naggati = "com.twitter" % "naggati" % "2.2.1"
  val finagle = "com.twitter" % "finagle-core" % finagleVersion
  val ostrich = "com.twitter" % "ostrich" % "4.10.1-SNAPSHOT"
  val finagle_ostrich4 = "com.twitter" % "finagle-ostrich4" % finagleVersion

  val scrooge_runtime = "com.twitter" % "scrooge-runtime" % "1.0.3"
  override def scroogeVersion = "2.2.0"

  // building docs seems to make scalac's head explode, so skip it for now. :(
  override def docSources = sources(mainJavaSourcePath ##)

  // for tests only:
  val specs = "org.scala-tools.testing" % "specs_2.8.1" % "1.6.7" % "test"
  val jmock = "org.jmock" % "jmock" % "2.4.0" % "test"
  val cglib = "cglib" % "cglib" % "2.1_3" % "test"
  val asm = "asm" % "asm" % "1.5.3" % "test"
  val objenesis = "org.objenesis" % "objenesis" % "1.1" % "test"
  val hamcrest = "org.hamcrest" % "hamcrest-all" % "1.1" % "test"

  override def mainClass = Some("net.lag.kestrel.Kestrel")

  override def pomExtra =
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>

  override def releaseBuild = !(projectVersion.toString contains "SNAPSHOT")

  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public")

  override def githubRemote = "github"

  override def validateConfigFilesSet = allConfigFiles

  // generate a jar that can be run for load tests.
  def loadTestJarFilename = "kestrel-tests-" + version.toString + ".jar"
  def loadTestPaths = ((testCompilePath ##) ***) +++ ((mainCompilePath ##) ***)
  def packageLoadTestsAction =
    packageTask(loadTestPaths, outputPath, loadTestJarFilename, packageOptions) && task {
      distPath.asFile.mkdirs()
      FileUtilities.copyFlat(List(outputPath / loadTestJarFilename), distPath, log).left.toOption
    }
  lazy val packageLoadTests = packageLoadTestsAction
  override def packageDistTask = packageLoadTestsAction && super.packageDistTask
}
