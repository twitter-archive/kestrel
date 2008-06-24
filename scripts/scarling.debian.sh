#!/bin/sh
#
# scarling init.d script.
#

QUEUE_PATH="/var/spool/starling"
SCALA_HOME="/usr/local/scala"
SCARLING_HOME="/usr/local/scarling"
AS_USER="daemon"

daemon_args="--name scarling --pidfile /var/run/scarling/scarling.pid"
JAVA_OPTS="-server -XX:+UseConcMarkSweepGC -XX:+UseParNewGC"


function running() {
    daemon $daemon_args --running
}

function find_java() {
    if [ ! -z "$JAVA_HOME" ]; then
        return
    fi
    potential=$(ls -r1d /opt/jdk /System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Home /usr/java/j* /usr/java/default 2>/dev/null)
    for p in $potential; do
        if [ -x $p/bin/java ]; then
            JAVA_HOME=$p
            break
        fi
    done
}


# dirs under /var/run can go away between reboots.
for p in /var/run/scarling /var/log/scarling $QUEUE_PATH; do
    if [ ! -d $p ]; then
        mkdir -p $p
        chmod 775 $p
        chown $AS_USER $p >/dev/null 2>&1 || true
    fi
done

find_java


case "$1" in
    start)
        echo -n "Starting scarling... "

        if [ ! -r $SCARLING_HOME/scarling.jar ]; then
            echo "FAIL"
            echo "*** scarling jar missing - not starting"
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
        daemon $daemon_args --user $AS_USER --stderr=/var/log/scarling/error -- ${JAVA_HOME}/bin/java ${JAVA_OPTS} -jar ${SCARLING_HOME}/scarling.jar
        sleep 1
        if running; then
            echo "done."
        else
            sleep 2
            if running; then
                echo "done."
            else
                # give up.
                echo "FAIL"
            fi
        fi
    ;;

    stop)
        echo -n "Stopping scarling... "
        if ! running; then
            echo "wasn't running."
            exit 0
        fi
        
        (echo "shutdown"; sleep 2) | telnet localhost 22122 >/dev/null 2>&1
        if running; then
            sleep 2
            if running; then
                echo "FAIL"
            else
                echo "done."
            fi
        else
            echo "done."
        fi
    ;;
    
    status)
        if running; then
            echo "scarling is running."
        else
            echo "scarling is NOT running."
        fi
    ;;

#    reload|force-reload)
#        log_daemon_msg "Reloading scarling..." "scarling"
#        ...
#        log_end_msg $?
#    ;;

    restart)
        $0 stop
        sleep 2
        $0 start
    ;;

    *)
        echo "Usage: /etc/init.d/scarling {start|stop|reload|restart|force-reload}"
        exit 1
    ;;
esac

exit 0
