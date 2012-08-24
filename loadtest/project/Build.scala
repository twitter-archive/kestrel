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
    scalaVersion := "2.9.2",

    libraryDependencies ++= Seq(
      "com.twitter" % "iago" % "0.5.2",
      "com.twitter" %% "scrooge-runtime" % "3.0.1"
    ),

    CompileThriftScrooge.scroogeVersion := "3.0.1",
    PackageDist.packageDistConfigFilesValidationRegex := None,
    publishArtifact in Test := true,

    fullRunInputTask(runParrot, Compile, parrotLauncherMain, "-f"),
    fullRunInputTask(killParrot, Compile, parrotLauncherMain, "-k", "-f")
  )
}
