#!/bin/bash
echo "Starting kestrel in development mode..."
java -server -Xmx1024m -Dstage=development -jar ./dist/kestrel/kestrel-@VERSION@.jar

