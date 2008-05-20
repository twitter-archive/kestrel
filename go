#!/bin/sh
LIBS=$SCALA_HOME/lib/scala-library.jar:$(echo libs/* | sed -e 's/ /:/g')
java -classpath $LIBS:build com.twitter.scarling.Scarling "$@"
