package net.lag.kestrel

import org.jboss.netty.util.Timeout
import org.jboss.netty.util.TimerTask
import java.util.concurrent._
import collection.mutable.{WeakHashMap, SynchronizedMap}
import collection.JavaConversions._

/**
 * Created by IntelliJ IDEA. User: jclites, Date: 8/13/2011 12:15 PM
 */
class ScheduledThreadPoolExecutorTimer(val poolSize : Int) extends org.jboss.netty.util.Timer {

  private val scheduledExecutorService = Executors.newScheduledThreadPool(poolSize).asInstanceOf[ScheduledThreadPoolExecutor]
  scheduledExecutorService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
  scheduledExecutorService.setContinueExistingPeriodicTasksAfterShutdownPolicy(false)

  private val _tasks = new WeakHashMap[Timeout, Null] with SynchronizedMap[Timeout, Null]

  def newTimeout(timerTask: TimerTask, l: Long, timeUnit: TimeUnit): Timeout = {
    val timeout = new CallableTimeout(timerTask)
    _tasks.put(timeout, null)
    timeout.schedule(l, timeUnit)
    timeout
  }

  def stop: java.util.Set[Timeout] = {
    scheduledExecutorService.shutdown
    _tasks.keySet.filter(item => !item.isExpired && !item.isCancelled)
  }


  private class CallableTimeout(val timerTask: TimerTask) extends Timeout with Callable[Null] {
    @volatile private var _future: ScheduledFuture[Null] = null

    def schedule(l: Long, timeUnit: TimeUnit): Unit = {
      _future = scheduledExecutorService.schedule(this.asInstanceOf[Callable[Null]], l, timeUnit)
    }

    def call = {
      timerTask.run(this)
      null
    }

    def getTimer = ScheduledThreadPoolExecutorTimer.this

    def getTask = timerTask

    def isExpired = _future.isDone

    def isCancelled = _future.isCancelled

    def cancel: Unit = _future.cancel(false)
  }

}