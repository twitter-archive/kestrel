name := "kestrel"

organization := "net.lag"

version := "2.1.5-SNAPSHOT"

initialize := "false"

libraryDependencies += "com.twitter" % "util-core"    % "1.12.4"

libraryDependencies += "com.twitter" % "util-eval"    % "1.12.4"

libraryDependencies += "com.twitter" % "util-logging" % "1.12.4"

libraryDependencies += "com.twitter" % "ostrich" % "4.10.0"

libraryDependencies += "com.twitter" % "naggati" % "2.1.1" intransitive() // allow custom netty

libraryDependencies += "org.jboss.netty" % "netty" % "3.2.6.Final"

libraryDependencies += "org.scala-tools.testing" % "specs_2.8.1" % "1.6.7" % "test"

libraryDependencies += "org.jmock" % "jmock" % "2.4.0" % "test"

libraryDependencies += "cglib" % "cglib" % "2.1_3" % "test"

libraryDependencies += "asm" % "asm" % "1.5.3" % "test"

libraryDependencies += "org.objenesis" % "objenesis" % "1.1" % "test"

libraryDependencies += "org.hamcrest" % "hamcrest-all" % "1.1" % "test"

resolvers += "twitter.com" at "http://maven.twttr.com/"

resolvers += "scala-tools" at "http://scala-tools.org/repo-releases/"

resolvers += "freemarker" at "http://freemarker.sourceforge.net/maven2/"
