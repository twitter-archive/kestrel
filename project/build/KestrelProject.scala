import sbt._
import com.twitter.sbt._

class KestrelProject(info: ProjectInfo) extends StandardServiceProject(info) with NoisyDependencies
  with SubversionPublisher
  with DefaultRepos
  with PublishSourcesAndJavadocs
  with PublishSite
{
  val utilCore =    "com.twitter" % "util-core"    % "1.12.4"
  val utilEval =    "com.twitter" % "util-eval"    % "1.12.4"
  val utilLogging = "com.twitter" % "util-logging" % "1.12.4"

  val ostrich = "com.twitter" % "ostrich" % "4.10.3"
  val naggati = "com.twitter" % "naggati" % "2.1.1" intransitive() // allow custom netty
  val netty   = "org.jboss.netty" % "netty" % "3.2.6.Final"

  // for tests only
  val specs = "org.scala-tools.testing" % "specs_2.8.1" % "1.6.7" % "test"
  val jmock = "org.jmock" % "jmock" % "2.4.0" % "test"
  val cglib = "cglib" % "cglib" % "2.1_3" % "test"
  val asm = "asm" % "asm" % "1.5.3" % "test"
  val objenesis = "org.objenesis" % "objenesis" % "1.1" % "test"
  val hamcrest = "org.hamcrest" % "hamcrest-all" % "1.1" % "test"

  // workaround bug in sbt that hides scala-compiler.
  override def filterScalaJars = false
  val what = "org.scala-lang" % "scala-compiler" % "2.8.1"

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

  override def subversionRepository = Some("https://svn.twitter.biz/maven-public")

  override def githubRemote = "github"

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

  // generate a distribution zip for release.
  def releaseDistTask = task {
    val releaseDistPath = "dist-release" / distName ##

    releaseDistPath.asFile.mkdirs()
    (releaseDistPath / "libs").asFile.mkdirs()
    (releaseDistPath / "config").asFile.mkdirs()

    FileUtilities.copyFlat(List(jarPath), releaseDistPath, log).left.toOption orElse
      FileUtilities.copyFlat(List(outputPath / loadTestJarFilename), releaseDistPath, log).left.toOption orElse
      FileUtilities.copyFlat(dependentJars.get, releaseDistPath / "libs", log).left.toOption orElse
      FileUtilities.copy(((configPath ***) --- (configPath ** "*.class")).get, releaseDistPath / "config", log).left.toOption orElse
      FileUtilities.copy((scriptsOutputPath ***).get, releaseDistPath, log).left.toOption orElse
      FileUtilities.zip((("dist-release" ##) / distName).get, "dist-release" / (distName + ".zip"), true, log)
  }
  val ReleaseDistDescription = "Creates a deployable zip file with dependencies, config, and scripts."
  lazy val releaseDist = releaseDistTask.dependsOn(`package`, makePom, copyScripts).describedAs(ReleaseDistDescription)


  lazy val putMany = task { args =>
    runTask(Some("net.lag.kestrel.load.PutMany"), testClasspath, args).dependsOn(testCompile)
  } describedAs "Run a load test on PUT."

  lazy val manyClients = task { args =>
    runTask(Some("net.lag.kestrel.load.ManyClients"), testClasspath, args).dependsOn(testCompile)
  } describedAs "Run a load test on many slow clients."

  lazy val flood = task { args =>
    runTask(Some("net.lag.kestrel.load.Flood"), testClasspath, args).dependsOn(testCompile)
  } describedAs "Run a load test on a flood of PUT/GET."

  lazy val packing = task { args =>
    runTask(Some("net.lag.kestrel.load.JournalPacking"), testClasspath, args).dependsOn(testCompile)
  } describedAs "Run a load test on journal packing."
}
