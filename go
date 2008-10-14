#!/bin/sh
SCALA_HOME=/Users/robey/code/scala/scala-2.7.1.final
LIBS=$SCALA_HOME/lib/scala-library.jar:$(echo dist/scarling-0.5/libs/* | sed -e 's/ /:/g'):dist/scarling-0.5/scarling-0.5.jar
java -server -classpath $LIBS:build com.twitter.scarling.Scarling "$@"
