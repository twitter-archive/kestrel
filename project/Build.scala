import sbt._
import Keys._
import com.twitter.sbt._

object Kestrel extends Build {
  val finagleVersion = "1.11.1"

  override lazy val settings = super.settings ++
    Seq(
      name := "kestrel",
      organization := "net.lag",
      version := "2.2.0-SNAPSHOT",
      scalaVersion := "2.9.1",

      // time-based tests cannot be run in parallel
      logBuffered in Test := false,
      parallelExecution in Test := false,

      libraryDependencies ++= Seq(
        "com.twitter" %% "ostrich" % "4.10.6",
        "com.twitter" %% "naggati" % "2.2.3" intransitive(), // allow custom netty
        "com.twitter" %% "finagle-core" % finagleVersion,
        "com.twitter" %% "finagle-ostrich4" % finagleVersion,
        "org.jboss.netty" % "netty" % "3.2.6.Final",
        "com.twitter" %% "scrooge-runtime" % "1.1.3",

        // for tests only:
        "org.scala-tools.testing" %% "specs" % "1.6.9" % "test",
        "org.jmock" % "jmock" % "2.4.0" % "test",
        "cglib" % "cglib" % "2.1_3" % "test",
        "asm" % "asm" % "1.5.3" % "test",
        "org.objenesis" % "objenesis" % "1.1" % "test",
        "org.hamcrest" % "hamcrest-all" % "1.1" % "test"
      ),

      CompileThriftScrooge.scroogeVersion := "1.1.7",

      mainClass := Some("net.lag.kestrel.Kestrel")
    )

  lazy val root = Project(
    id = "kestrel",
    base = file("."),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      CompileThriftScrooge.newSettings
  )
}
