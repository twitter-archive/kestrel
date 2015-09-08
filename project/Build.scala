import sbt._
import Keys._
import com.twitter.sbt._

object Kestrel extends Build {
  val finagleVersion = "6.4.1"

  lazy val root = Project(
    id = "kestrel",
    base = file("."),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      SubversionPublisher.newSettings ++
      CompileThriftScrooge.newSettings
  ).settings(
    name := "kestrel",
    organization := "net.lag",
    version := "2.4.8-SNAPSHOT",
    scalaVersion := "2.9.2",

    // time-based tests cannot be run in parallel
    logBuffered in Test := false,
    parallelExecution in Test := false,

    resolvers += "twitter.com" at "https://maven.twttr.com/",

    libraryDependencies ++= Seq(
      "com.twitter" % "ostrich" % "8.2.9",
      "com.twitter" %% "naggati" % "4.1.0",
      "com.twitter" % "finagle-core" % finagleVersion,
      "com.twitter" % "finagle-ostrich4" % finagleVersion,
      "com.twitter" % "finagle-thrift" % finagleVersion, // override scrooge's version
      "com.twitter" %% "scrooge-runtime" % "3.0.1",
      "com.twitter.common.zookeeper" % "server-set" % "1.0.16",
      // for tests only:
      "junit" % "junit" % "4.10" % "test",
      "org.mockito" % "mockito-all" % "1.9.5" % "test",
      "org.scala-tools.testing" %% "specs" % "1.6.9" % "test",
      "org.jmock" % "jmock" % "2.4.0" % "test",
      "cglib" % "cglib" % "2.1_3" % "test",
      "asm" % "asm" % "1.5.3" % "test",
      "org.objenesis" % "objenesis" % "1.1" % "test",
      "org.hamcrest" % "hamcrest-all" % "1.1" % "test",
      "org.scalatest" % "scalatest_2.9.0" % "1.9.2" % "test"
    ),

    mainClass in Compile := Some("net.lag.kestrel.Kestrel"),

    CompileThriftScrooge.scroogeVersion := "3.0.1",
    PackageDist.packageDistConfigFilesValidationRegex := Some(".*"),
    SubversionPublisher.subversionRepository := Some("https://svn.twitter.biz/maven-public"),
    publishArtifact in Test := true
  )
}
