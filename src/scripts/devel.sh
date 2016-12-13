#!/bin/sh
echo "Starting kestrel in development mode..."

if ! [ -d /var/log/kestrel ] || ! [ -d /var/spool/kestrel ] ; then
    echo "Creating some dirs as root"
    sudo mkdir -p /var/log/kestrel /var/spool/kestrel
    sudo chown $(whoami):  /var/log/kestrel /var/spool/kestrel
fi

echo "logs:   /var/log/kestrel"
echo "queues: /var/spool/kestrel"

# find jar no matter what the root dir name
SCRIPT_DIR=$(cd `dirname "$0"`; pwd)
ROOT_DIR=`dirname "$SCRIPT_DIR"`
source $SCRIPT_DIR/base.sh
JAR=$(find_jar $ROOT_DIR)
java -server -Xmx1024m -Dstage=development -jar $JAR -f $ROOT_DIR/config/development.scala
