#!/bin/sh
SCALA_HOME=/Users/robey/code/scala/scala-2.7.1.final
LIBS=$SCALA_HOME/lib/scala-library.jar:$(echo libs/* | sed -e 's/ /:/g')
java -server -classpath $LIBS:build com.twitter.scarling.Scarling "$@"
