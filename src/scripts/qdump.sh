#!/bin/sh
DIST_HOME="$(dirname $0)/.."
java -server -classpath @DIST_CLASSPATH@ scala.tools.nsc.MainGenericRunner net.lag.kestrel.tools.QDumper "$@"
