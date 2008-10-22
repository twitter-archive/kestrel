package com.twitter.scarling

// i'm a little weirded out that java doesn't have Event yet.
class Event {
  private var _flag = false
  private val _lock = new Object

  def clear: Unit = _lock.synchronized { _flag = false }

  def set: Unit = _lock.synchronized {
    _flag = true
    _lock.notifyAll
  }

  def isSet = _lock.synchronized { _flag }

  def waitFor(timeout: Int): Unit = _lock.synchronized {
    if (! _flag) {
      _lock.wait(timeout)
    }
  }

  def waitFor: Unit = waitFor(0)
}
