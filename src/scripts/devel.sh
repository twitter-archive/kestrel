#!/bin/sh
echo "Starting kestrel in development mode..."

# find jar no matter what the root dir name
SCRIPT_DIR=$(cd `dirname "$0"`; pwd)
ROOT_DIR=`dirname "$SCRIPT_DIR"`

java -server -Xmx1024m -Dstage=development -jar "$ROOT_DIR"/@DIST_NAME@-@VERSION@.jar
