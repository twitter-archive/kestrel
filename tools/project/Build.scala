import sbt._
import Keys._
import com.twitter.sbt._

object KestrelZookeeper extends Build {
  lazy val root = Project(
    id = "kestrel-zookeeper",
    base = file("."),
    settings = Project.defaultSettings ++
      StandardProject.newSettings
  ).settings(
    name := "kestrel-pages",
    organization := "net.lag",
    version := "1.0.0-SNAPSHOT",
    scalaVersion := "2.9.2",

    libraryDependencies ++= Seq(
      "org.markdownj" % "markdownj" % "0.3.0-1.0.2b4",
      "org.freemarker" % "freemarker" % "2.3.19"
    ),

    mainClass := Some("net.lag.kestrel.tools.PublishSite")
  )
}

