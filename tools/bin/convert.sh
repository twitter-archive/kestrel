#!/bin/bash

TOOLS_DIR=$(cd `dirname $0`/..; pwd)
TOOLS_JAR="$TOOLS_DIR/dist/kestrel-pages/kestrel-pages_2.9.2-1.0.0-SNAPSHOT.jar"
if [ ! -f "$TOOLS_JAR" ]; then
    pushd "$TOOLS_DIR"
    sbt11 package-dist
    popd
fi
java -jar "$TOOLS_JAR" "$@"
