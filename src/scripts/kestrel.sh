#!/bin/sh
#
# kestrel init.d script.
#
# All java services require the same directory structure:
#   /usr/local/$APP_NAME
#   /var/log/$APP_NAME
#   /var/run/$APP_NAME

APP_NAME="kestrel"
ADMIN_PORT="2223"
VERSION="@VERSION@"
SCALA_VERSION="2.9.1"
APP_HOME="/usr/local/$APP_NAME/current"
INITIAL_SLEEP=15

JAR_NAME="${APP_NAME}_${SCALA_VERSION}-${VERSION}.jar"
STAGE="production"
FD_LIMIT="262144"

HEAP_OPTS="-Xmx4096m -Xms4096m -XX:NewSize=768m"
GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
GC_TRACE="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC"
GC_LOG="-Xloggc:/var/log/$APP_NAME/gc.log"
DEBUG_OPTS="-XX:ErrorFile=/var/log/$APP_NAME/java_error%p.log"

# allow a separate file to override settings.
test -f /etc/sysconfig/kestrel && . /etc/sysconfig/kestrel

JAVA_OPTS="-server -Dstage=$STAGE $GC_OPTS $GC_TRACE $GC_LOG $HEAP_OPTS $DEBUG_OPTS"

pidfile="/var/run/$APP_NAME/$APP_NAME.pid"
# This second pidfile exists for legacy purposes, from the days when kestrel
# was started by daemon(1)
daemon_pidfile="/var/run/$APP_NAME/$APP_NAME-daemon.pid"


running() {
  curl -m 5 -s "http://localhost:$ADMIN_PORT/ping.txt" > /dev/null 2> /dev/null
}

find_java() {
  if [ ! -z "$JAVA_HOME" ]; then
    return
  fi
  for dir in /opt/jdk /System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Home /usr/java/default; do
    if [ -x $dir/bin/java ]; then
      JAVA_HOME=$dir
      break
    fi
  done
}

find_java


case "$1" in
  start)
    echo -n "Starting $APP_NAME... "

    if [ ! -r $APP_HOME/$JAR_NAME ]; then
      echo "FAIL"
      echo "*** $APP_NAME jar missing: $APP_HOME/$JAR_NAME - not starting"
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

    TIMESTAMP=$(date +%Y%m%d%H%M%S);
    # Move the existing gc log to a timestamped file in case we want to examine it.
    # We must do this here because we have no option to append this via the JVM's
    # command line args.
    if [ -f /var/log/$APP_NAME/gc.log ]; then
      mv /var/log/$APP_NAME/gc.log /var/log/$APP_NAME/gc_$TIMESTAMP.log;
    fi

    ulimit -n $FD_LIMIT || echo -n " (no ulimit)"
    ulimit -c unlimited || echo -n " (no coredump)"

    sh -c "echo "'$$'" > $pidfile; echo "'$$'" > $daemon_pidfile; exec ${JAVA_HOME}/bin/java ${JAVA_OPTS} -jar ${APP_HOME}/${JAR_NAME} >> /var/log/$APP_NAME/stdout 2>> /var/log/$APP_NAME/error" &
    disown %-
    sleep $INITIAL_SLEEP

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
    echo -n "Stopping $APP_NAME... "
    if ! running; then
      echo "wasn't running."
      exit 0
    fi

    curl -m 5 -s http://localhost:${ADMIN_PORT}/shutdown.txt > /dev/null
    tries=0
    while running; do
      tries=$((tries + 1))
      if [ $tries -ge 15 ]; then
        echo "FAILED SOFT SHUTDOWN, TRYING HARDER"
        if [ -f $pidfile ]; then
          kill $(cat $pidfile)
        else
          echo "CAN'T FIND PID, TRY KILL MANUALLY"
          exit 1
        fi
        hardtries=0
        while running; do
          hardtries=$((hardtries + 1))
          if [ $hardtries -ge 5 ]; then
            echo "FAILED HARD SHUTDOWN, TRY KILL -9 MANUALLY"
            kill -9 $(cat $pidfile)
          fi
          sleep 1
        done
      fi
      sleep 1
    done
    echo "done."
  ;;

  status)
    if running; then
      echo "$APP_NAME is running."
    else
      echo "$APP_NAME is NOT running."
    fi
  ;;

  restart)
    $0 stop
    sleep 2
    $0 start
  ;;

  *)
    echo "Usage: /etc/init.d/${APP_NAME}.sh {start|stop|restart|status}"
    exit 1
  ;;
esac

exit 0
