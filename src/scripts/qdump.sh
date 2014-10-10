#!/bin/sh
DIST_HOME="$(dirname $(readlink $0 || echo $0))/.."
source $DIST_HOME/scripts/base.sh
JAR=$(find_jar $DIST_HOME)
java -server -classpath "$DIST_HOME/libs/*:$JAR" net.lag.kestrel.tools.QDumper "$@"
