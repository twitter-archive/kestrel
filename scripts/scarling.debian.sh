#!/bin/sh
#
# scarling init.d script for debian/ubuntu systems.
#

QUEUE_PATH=/var/spool/scarling
SCALA_HOME=/usr/local/scala
SCARLING_HOME=/usr/local/scarling

. /lib/lsb/init-functions
DISTRO=$(lsb_release -is 2>/dev/null || echo Debian)

case "$1" in
    start)
        log_daemon_msg "Starting scarling..." "scarling"

        # dirs under /var/run can go away between reboots.
        for p in /var/run/scarling $QUEUE_PATH; do
            mkdir -p $p
            chmod 775 $p
            chown root:daemon $p >/dev/null 2>&1 || true
        done

        if [ ! -r $SCARLING_HOME/scarling.jar ]; then
            log_action_msg "scarling jar missing - not starting"
            log_end_msg 1
            exit 1
        fi
        
        daemon --name scarling --pidfile /var/run/scarling/scarling.pid --user daemon.daemon -- java -jar $SCARLING_HOME/scarling.jar
        sleep 1
        if daemon --name scarling running; then
            log_end_msg 0
        else
            sleep 2
            if daemon --name scarling running; then
                log_end_msg 0
            else
                # give up.
                log_end_msg 1
            fi
        fi
    ;;

    stop)
        log_daemon_msg "Stopping scarling..." "scarling"
        echo "shutdown" | telnet localhost 22122
        log_end_msg $?
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
