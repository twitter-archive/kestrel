package main.java.net.lag.kestrel;

import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by IntelliJ IDEA. User: jclites, Date: 8/13/2011 12:15 PM
 */
public class ScheduledThreadPoolExecutorTimer implements org.jboss.netty.util.Timer {
    private ScheduledThreadPoolExecutor scheduledExecutorService;

    public ScheduledThreadPoolExecutorTimer(int poolSize) {
        scheduledExecutorService = (ScheduledThreadPoolExecutor)Executors.newScheduledThreadPool(poolSize);
        scheduledExecutorService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduledExecutorService.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }

    private class CallableTimeout implements Timeout, Callable<Void> {
        private final TimerTask _timerTask;
        private volatile ScheduledFuture<Void> _future;

        private CallableTimeout(TimerTask timerTask) {
            _timerTask = timerTask;
        }

        private void schedule(final long l, final TimeUnit timeUnit) {
            _future = scheduledExecutorService.schedule(this, l, timeUnit);
        }

        public Void call() throws Exception {
            _timerTask.run(this);
            return null;
        }

        public Timer getTimer() {
            return ScheduledThreadPoolExecutorTimer.this;
        }

        public TimerTask getTask() {
            return _timerTask;
        }

        public boolean isExpired() {
            return _future.isDone();
        }

        public boolean isCancelled() {
            return _future.isCancelled();
        }

        public void cancel() {
            _future.cancel(false);
        }
    }

    private static final Object _dummy = new Object();
    private final Map<Timeout, Object> _tasks = Collections.synchronizedMap(new WeakHashMap<Timeout, Object>());

    public Timeout newTimeout(final TimerTask timerTask, final long l, final TimeUnit timeUnit) {
        final CallableTimeout timeout = new CallableTimeout(timerTask);
        _tasks.put(timeout, _dummy);
        timeout.schedule(l, timeUnit);
        return timeout;
    }

    public Set<Timeout> stop() {
        scheduledExecutorService.shutdown();
        //final BlockingQueue<Runnable> queue = scheduledExecutorService.getQueue();
        Set<Timeout> result = new HashSet<Timeout>();
        for( Timeout timeout : _tasks.keySet() ) {
            if( !timeout.isCancelled() && !timeout.isExpired() ) {
                result.add(timeout);
            }
        }

        return result;
    }
}
