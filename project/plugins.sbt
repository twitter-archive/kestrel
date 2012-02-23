
resolvers += "local" at "file:/Users/robey/.m2/repo/"

resolvers += "twitter" at "http://artifactory.local.twitter.com/repo"

addSbtPlugin("com.twitter" % "standard-project2" % "0.0.4-SNAPSHOT")

addSbtPlugin("com.twitter" % "sbt-scrooge2" % "0.0.1")
