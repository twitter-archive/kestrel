#!/bin/sh
#
# kestrel init.d script.
#

QUEUE_PATH="/var/spool/kestrel"
SCARLING_HOME="/usr/local/kestrel"
AS_USER="daemon"
VERSION="0.5"
DAEMON="/usr/local/bin/daemon"

daemon_args="--name kestrel --pidfile /var/run/kestrel.pid"
HEAP_OPTS="-Xmx2048m -Xms1024m -XX:NewSize=256m"
# -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false 
JAVA_OPTS="-server -verbosegc -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC $HEAP_OPTS"


function running() {
  $DAEMON $daemon_args --running
}

function find_java() {
  if [ ! -z "$JAVA_HOME" ]; then
    return
  fi
  potential=$(ls -r1d /opt/jdk /System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Home /usr/java/default /usr/java/j* 2>/dev/null)
  for p in $potential; do
    if [ -x $p/bin/java ]; then
      JAVA_HOME=$p
      break
    fi
  done
}


# dirs under /var/run can go away between reboots.
for p in /var/run/kestrel /var/log/kestrel $QUEUE_PATH; do
  if [ ! -d $p ]; then
    mkdir -p $p
    chmod 775 $p
    chown $AS_USER $p >/dev/null 2>&1 || true
  fi
done

find_java


case "$1" in
  start)
    echo -n "Starting kestrel... "

    if [ ! -r $SCARLING_HOME/kestrel-$VERSION.jar ]; then
      echo "FAIL"
      echo "*** kestrel jar missing - not starting"
      exit 1
    fi
    if [ ! -x $JAVA_HOME/bin/java ]; then
      echo "FAIL"
      echo "*** $JAVA_HOME/bin/java doesn't exist -- check JAVA_HOME?"
      exit 1
    fi
    if running; then
      echo "already running."
      exit 0
    fi
    
    ulimit -n 8192 || echo -n " (no ulimit)"
    $DAEMON $daemon_args --user $AS_USER --stdout=/var/log/kestrel/stdout --stderr=/var/log/kestrel/error -- ${JAVA_HOME}/bin/java ${JAVA_OPTS} -jar ${SCARLING_HOME}/kestrel-${VERSION}.jar
    tries=0
    while ! running; do
      tries=$((tries + 1))
      if [ $tries -ge 5 ]; then
        echo "FAIL"
        exit 1
      fi
      sleep 1
    done
    echo "done."
  ;;

  stop)
    echo -n "Stopping kestrel... "
    if ! running; then
      echo "wasn't running."
      exit 0
    fi
    
    (echo "shutdown"; sleep 2) | telnet localhost 22133 >/dev/null 2>&1
    tries=0
    while running; do
      tries=$((tries + 1))
      if [ $tries -ge 5 ]; then
        echo "FAIL"
        exit 1
      fi
      sleep 1
    done
    echo "done."
  ;;
  
  status)
    if running; then
      echo "kestrel is running."
    else
      echo "kestrel is NOT running."
    fi
  ;;

  restart)
    $0 stop
    sleep 2
    $0 start
  ;;

  *)
    echo "Usage: /etc/init.d/kestrel {start|stop|restart|status}"
    exit 1
  ;;
esac

exit 0
