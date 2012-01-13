#!/bin/sh
DIST_HOME="$(dirname $0)/../.."
java -server -classpath @DIST_CLASSPATH@ net.lag.kestrel.oldjournal.QPacker "$@"
