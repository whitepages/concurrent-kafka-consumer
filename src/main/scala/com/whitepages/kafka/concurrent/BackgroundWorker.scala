package com.whitepages.kafka.concurrent

import scala.concurrent.duration.FiniteDuration


trait StartStop {
  def start(): this.type
  def running(): Boolean
  def shutdown(): Unit
}

trait StopableRunnable extends Runnable {
  def shuttingDown: () => Boolean
  def processOnce(): Long  // return the time in millis to wait before the next doWork
  override def run(): Unit = {
    while(!shuttingDown()) {
      val delay = processOnce()
      if (delay > 0) Thread.sleep(delay)
    }
  }
}

trait BackgroundWorker extends StartStop {
  var shutdownFlag: Boolean = false
  var workerException: Option[Throwable] = None
  var workerThread: Option[Thread] = None
  class UncaughtExceptionHandler extends Thread.UncaughtExceptionHandler {
    override def uncaughtException(t: Thread, e: Throwable): Unit = {
      workerException = workerException.orElse(Some(e))
      //println("Uncaught exception: " + e)
    }
  }

  def backgroundWorker(shutdownHook: () => Boolean): StopableRunnable

  override def start() = {
    if (!running) {
      workerThread = Some(new Thread(backgroundWorker(() => shutdownFlag)))
      workerThread.foreach(_.setUncaughtExceptionHandler(new UncaughtExceptionHandler()))
      workerThread.get.start()
    }
    this
  }
  override def shutdown() = {
    shutdownFlag = true
    workerThread.foreach(_.join())
  }
  override def running() = workerThread match {
    case Some(t) => t.isAlive
    case None => false
  }
}
