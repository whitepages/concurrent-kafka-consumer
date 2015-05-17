package com.whitepages.kafka.concurrent

import org.scalatest.{Matchers, BeforeAndAfterEach, BeforeAndAfterAll, FunSpec}

class TestBackgroundWorker extends FunSpec with Matchers {
  val sleepDelayMs = 50

  class TestBackgroundWorkerImpl(workedIndicator: () => Unit) extends BackgroundWorker {
    override def backgroundWorker(shutdownHook: () => Boolean): StopableRunnable = {
      new StopableRunnable {
        override def shuttingDown = shutdownHook
        override def processOnce(): Long = {
          workedIndicator()
          //println("Working")
          sleepDelayMs
        }
      }
    }
  }

  describe("background worker should have a lifecycle") {
    it("should start/stop") {
      var workedIndicator = 0
      val expectedWorkLoops = 10
      val bgw = new TestBackgroundWorkerImpl(() => workedIndicator = workedIndicator + 1)
      bgw.start()
      Thread.sleep(sleepDelayMs * expectedWorkLoops)
      bgw.running() should be(true)
      workedIndicator should be(expectedWorkLoops +- expectedWorkLoops / 5) // 20% error is ok?
      bgw.shutdown()
      Thread.sleep(sleepDelayMs * 2)
      bgw.running() should be(false)
    }
  }

}
