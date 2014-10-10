#!/bin/bash
#
# kestrel foreground script
#
# All java services require the same directory structure:
#   /usr/local/$APP_NAME
#   /var/log/$APP_NAME
#   /var/run/$APP_NAME

APP_NAME="kestrel"
ADMIN_PORT="2223"
APP_HOME="."
INITIAL_SLEEP=15

source base.sh
JAR=$(find_jar $APP_HOME)
FD_LIMIT="262144"

if [ "$HEAP_OPTS" == "" ]; then
    HEAP_OPTS="-Xmx16G -XX:NewSize=2G"
fi
if [ "$GC_OPTS" == "" ]; then
    GC_OPTS="-XX:+UseParallelOldGC -XX:+UseAdaptiveSizePolicy -XX:MaxGCPauseMillis=1000 -XX:GCTimeRatio=99 -XX:NewSize=2G"
fi
if [ "$GC_TRACE" == "" ]; then
    GC_TRACE="-verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps"
fi
if [ "$GC_LOG" == "" ]; then
    GC_LOG="-Xloggc:logs/gc.log"
fi
if [ "$DEBUG_OPTS" == "" ]; then
    DEBUG_OPTS="-XX:ErrorFile=java_error%p.log"
fi

# allow a separate file to override settings.
test -f /etc/sysconfig/kestrel && . /etc/sysconfig/kestrel

JAVA_OPTS="-server $GC_OPTS $GC_TRACE $GC_LOG $HEAP_OPTS $DEBUG_OPTS"

pidfile="$APP_NAME.pid"

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

mkdir -p logs

echo "Starting $APP_NAME... "

if [ ! -x $JAVA_HOME/bin/java ]; then
  echo "FAIL"
  echo "*** $JAVA_HOME/bin/java doesn't exist -- check JAVA_HOME?"
  exit 1
fi

TIMESTAMP=$(date +%Y%m%d%H%M%S);
# Move the existing gc log to a timestamped file in case we want to examine it.
# We must do this here because we have no option to append this via the JVM's
# command line args.
GC_LOGFILE=`echo $GC_LOG | cut -f2 -d:`
GC_LOGDIR=`dirname $GC_LOGFILE`
if [ -f $GC_LOGFILE ]; then
  mv $GC_LOGFILE $GC_LOGDIR/gc_$TIMESTAMP.log;
fi

ulimit -n $FD_LIMIT || echo " (no ulimit)"
ulimit -c unlimited || echo " (no coredump)"

echo "'$$'" > $pidfile
exec ${JAVA_HOME}/bin/java ${JAVA_OPTS} -jar $JAR "$@"
