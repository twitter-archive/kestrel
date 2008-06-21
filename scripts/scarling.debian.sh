#!/bin/sh
#
# scarling init.d script for debian/ubuntu systems.
#

QUEUE_PATH=/var/spool/starling
SCALA_HOME=/usr/local/scala
SCARLING_HOME=/usr/local/scarling

# this hackery is just to let it work on my mac.
if [ "$2" = "--no-lsb" ]; then
    function log_daemon_msg() {
        /bin/echo -n "$1   "
    }
    function log_warning_msg() {
        echo "*** $1"
    }
    function log_end_msg() {
        if [ "$1" = "0" ]; then
            echo "success"
        else
            echo "FAILED"
        fi
    }
else
    trap "$0 $1 --no-lsb" EXIT
    . /lib/lsb/init-functions
    trap - EXIT
fi

DISTRO=$(lsb_release -is 2>/dev/null || echo Debian)
common_args="--name scarling --pidfile /var/run/scarling/scarling.pid"

case "$1" in
    start)
        log_daemon_msg "Starting scarling..." "scarling"

        # dirs under /var/run can go away between reboots.
        for p in /var/run/scarling /var/log/scarling $QUEUE_PATH; do
            mkdir -p $p
            chmod 775 $p
            chown root:daemon $p >/dev/null 2>&1 || true
        done

        if [ ! -r $SCARLING_HOME/scarling.jar ]; then
            log_warning_msg "scarling jar missing - not starting"
            log_end_msg 1
            exit 1
        fi
        
        daemon $common_args --user daemon.daemon --stderr=/var/log/scarling/error -- java -jar ${SCARLING_HOME}/scarling.jar
        sleep 1
        if daemon $common_args --running; then
            log_end_msg 0
        else
            sleep 2
            if daemon $common_args --running; then
                echo yay2
                log_end_msg 0
            else
                # give up.
                log_end_msg 1
            fi
        fi
    ;;

    stop)
        log_daemon_msg "Stopping scarling..." "scarling"
        (echo "shutdown"; sleep 2) | telnet localhost 22122 >/dev/null 2>&1
        if daemon $common_args --running; then
            sleep 2
            if daemon $common_args --running; then
                log_end_msg 1
            else
                log_end_msg 0
            fi
        else
            log_end_msg 0
        fi
    ;;
    
    status)
        if daemon $common_args --running; then
            log_action_msg "scarling is running."
        else
            log_action_msg "scarling is NOT running."
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
        log_action_msg "Usage: /etc/init.d/scarling {start|stop|reload|restart|force-reload}"
        exit 1
    ;;
esac

exit 0
