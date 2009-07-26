#!/bin/sh
DIST_HOME="$(dirname $0)/.."
java -classpath @DIST_CLASSPATH@ scala.tools.nsc.MainGenericRunner net.lag.kestrel.QDumper "$@"
