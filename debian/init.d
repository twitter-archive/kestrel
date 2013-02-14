#!/bin/sh
### BEGIN INIT INFO
# Provides:          kestrel
# Required-Start:    $network $local_fs $remote_fs
# Required-Stop:     $network $local_fs $remote_fs
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Simple, distributed message queue.
# Description:        Kestrel is a simple, distributed message queue written on
#                     the JVM, based on Blaine Cook's "starling".
### END INIT INFO

# Author: Kyle Ambroff <ambroff@jawbone.com>

# PATH should only include /usr/* if it runs after the mountnfs.sh script
PATH=/sbin:/usr/sbin:/bin:/usr/bin
DESC=kestrel
NAME=kestrel
PIDFILE=/var/run/$NAME.pid
SCRIPTNAME=/etc/init.d/$NAME
JAR_NAME=/usr/share/kestrel/kestrel.jar

# Default values from /etc/default/$NAME
START_KESTREL=1
KESTREL_USER="kestrel"
KESTREL_GROUP="kestrel"
ADMIN_PORT="2223"
FD_LIMIT="262144"
HEAP_OPTS="-Xmx4096m -Xms4096m -XX:NewSize=768m"
GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
GC_TRACE="-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC"
GC_LOG="-Xloggc:/var/log/$NAME/gc.log"
DEBUG_OPTS="-XX:ErrorFile=/var/log/$NAME/java_error.log"
JAVA_HOME="/usr/lib/jvm/default-java"
JAVA="$JAVA_HOME/bin/java"

DEFAULT_FILE=/etc/default/kestrel
if test -f "$DEFAULT_FILE"
then
    . $DEFAULT_FILE
fi

if [ "$START_KESTREL" -ne 1 ]; then
    echo "kestrel disabled in $DEFAULT_FILE. FIle should include START_KESTREL=1 to enable. Exiting."
    exit 0
fi

# Load the VERBOSE setting and other rcS variables
. /lib/init/vars.sh

# Define LSB log_* functions.
. /lib/lsb/init-functions

# Assemble daemon options
JAVA_OPTS="-server -Dstage=config $GC_OPTS $GC_TRACE $GC_LOG $HEAP_OPTS $DEBUG_OPTS"

#
# Test whether the daemon is running.
#
is_running()
{
    kill -0 $(cat $PIDFILE)
}

#
# Function that starts the daemon/service
#
# Return
#   0 if daemon has been started
#   1 if daemon was already running
#   2 if daemon could not be started
#
do_start()
{
	# Make sure we're running as root.
	# correct.
	if [ $(id -u) -ne 0 ]; then
		echo "Only root can start $NAME."
		exit 1
	fi

    # Set fd limit and enable core dumps
    ulimit -n $FD_LIMIT
    ulimit -c unlimited

    start-stop-daemon --start --quiet \
        --pidfile $PIDFILE \
        --exec $JAVA \
        --test > /dev/null \
        || return 1

    start-stop-daemon --start --quiet --make-pidfile --background \
        --pidfile $PIDFILE \
        --chuid $KESTREL_USER:$KESTREL_GROUP \
        --exec $JAVA -- $JAVA_OPTS -jar $JAR_NAME \
        || return 2

    local tries=0
    while ! is_running; do
        tries=$(expr $tries + 1)
        if [ $tries -gt 10 ]; then
            return 1
        fi
        sleep 1
    done

    # Started successfull.
    return 0
}

#
# Function that stops the daemon/service
#
# Return
#   0 if daemon has been stopped
#   1 if daemon was already stopped
#   2 if daemon could not be stopped
#   other if a failure occurred
#
do_stop()
{
    # First try to tell kestrel to shutdown gracefully.
    curl -f -m 5 -s http://localhost:${ADMIN_PORT}/shutdown.txt > /dev/null
	if [ $? -eq 0 ]; then
		# It received our request, so give it a few seconds to shut down.
		sleep 3
	fi

    # Now shut down and clean up in case it didn't stop.
    start-stop-daemon --stop --quiet \
        --retry=TERM/30/KILL/5 \
        --pidfile $PIDFILE 2> /dev/null
    RETVAL="$?"
    [ "$RETVAL" = 2 ] && return 2

    # Wait for children to finish too if this is a daemon that forks
    # and if the daemon is only ever run from this initscript.
    # If the above conditions are not satisfied then add some other code
    # that waits for the process to drop all resources that could be
    # needed by services started subsequently.  A last resort is to
    # sleep for some time.
    start-stop-daemon --stop --quiet --oknodo --retry=0/30/KILL/5 --exec $JAVA
    [ "$?" = 2 ] && return 2

    # Many daemons don't delete their pidfiles when they exit.
    rm -f $PIDFILE
    return "$RETVAL"
}

case "$1" in
  start)
    [ "$VERBOSE" != no ] && log_daemon_msg "Starting $DESC " "$NAME"
    do_start
    case "$?" in
        0|1) [ "$VERBOSE" != no ] && log_end_msg 0 ;;
        2) [ "$VERBOSE" != no ] && log_end_msg 1 ;;
    esac
  ;;
  stop)
    [ "$VERBOSE" != no ] && log_daemon_msg "Stopping $DESC" "$NAME"
    do_stop
    case "$?" in
        0|1) [ "$VERBOSE" != no ] && log_end_msg 0 ;;
        2) [ "$VERBOSE" != no ] && log_end_msg 1 ;;
    esac
    ;;
  status)
       status_of_proc "$NAME" "$NAME" || true
       ;;
  restart|force-reload)
    log_daemon_msg "Restarting $DESC" "$NAME"
    do_stop
    case "$?" in
      0|1)
        do_start
        case "$?" in
            0) log_end_msg 0 ;;
            1) log_end_msg 1 ;; # Old process is still running
            *) log_end_msg 1 ;; # Failed to start
        esac
        ;;
      *)
        # Failed to stop
        log_end_msg 1
        ;;
    esac
    ;;
  *)
    echo "Usage: $SCRIPTNAME {start|stop|status|restart|force-reload}" >&2
    exit 3
    ;;
esac

exit 0
