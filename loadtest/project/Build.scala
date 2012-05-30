import sbt._
import Keys._
import com.twitter.sbt._

object Kestrel extends Build {
  val parrotLauncherMain = "com.twitter.parrot.launcher.LauncherMain"
  val runParrot = InputKey[Unit]("run-parrot")
  val killParrot = InputKey[Unit]("kill-parrot")

  lazy val root = Project(
    id = "kestrel_loadtest",
    base = file("."),
    settings = Project.defaultSettings ++
      StandardProject.newSettings ++
      SubversionPublisher.newSettings ++
      CompileThriftScrooge.newSettings
  ).settings(
    name := "kestrel_loadtest",
    organization := "net.lag",
    version := "1.0.0-SNAPSHOT",
    scalaVersion := "2.8.1",

    libraryDependencies ++= Seq(
      "com.twitter" % "iago" % "0.4.7",
      "com.twitter" %% "scrooge-runtime" % "1.1.3"
    ),

    CompileThriftScrooge.scroogeVersion := "1.1.7",
    PackageDist.packageDistConfigFilesValidationRegex := None,
    publishArtifact in Test := true,

    fullRunInputTask(runParrot, Compile, parrotLauncherMain, "-f"),
    fullRunInputTask(killParrot, Compile, parrotLauncherMain, "-k", "-f")
  )
}
