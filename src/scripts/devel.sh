#!/bin/bash
echo "Starting kestrel in development mode..."
DIST_HOME="$(dirname $(readlink $0 || echo $0))/../.."
java -server -Xmx1024m -Dstage=development -jar $DIST_HOME/kestrel/kestrel-@VERSION@.jar

