package com.whitepages.kafka.concurrent

import com.whitepages.kafka.concurrent.Ack.{AckedMessage, AckType, AckableMessage}
import com.whitepages.kafka.concurrent.messagesource.IteratorAckingMessageSource
import org.scalatest.{BeforeAndAfterEach, Matchers, FunSpec}

import scala.collection.mutable
import scala.concurrent.{Await, Future, blocking}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class TestAsyncBatchAckingMessageSource extends FunSpec with Matchers with BeforeAndAfterEach {

  def asyncBridge() = new HandlingAsyncAckingBridge[Int]((l) => l.foreach(failures.enqueue(_)))
  def msgGenerator() = new IteratorAckingMessageSource[Int](Range(0, messageSourceSize).iterator)
  def shutdownIndicator() = false

  val messageSourceSize = 100
  val failures = new mutable.Queue[AckedMessage[Int]]()
  var bridge = asyncBridge()
  var generator = msgGenerator()
  override def beforeEach() = {
    bridge = asyncBridge()
    generator = msgGenerator()
    failures.clear()
  }

  class TestAsyncBatchAckingMessageSourceImpl(override val asyncBridge: AsyncAckingBridge[Int], override val messageSource: BatchAckingMessageSource[Int])
    extends AsyncBatchAckingMessageSource[Int] {
    override val ackTimeout: FiniteDuration = 1.second
    override val desiredCommitThreshold: Int = messageSourceSize / 10

    override def shuttingDown: () => Boolean = shutdownIndicator
  }

  describe("when processing normally") {
    it("should hand out messages") {
      val messageSource = new TestAsyncBatchAckingMessageSourceImpl(bridge, generator)
      for (i <- 0 until messageSource.desiredCommitThreshold) {
        val response = fetchMessage(bridge, messageSource)
        response.delay should be(0)
        response.msg.msg should be(i)
      }
    }
    it("should process acks") {
      val messageSource = new TestAsyncBatchAckingMessageSourceImpl(bridge, generator)
      for (i <- 0 until messageSource.desiredCommitThreshold) {
        val response = fetchMessageAndAck(bridge, messageSource)
        response.delay should be(0)
        response.msg.msg should be(i)
      }
      bridge.outstandingAcks.size() should be(messageSource.desiredCommitThreshold)
      fetchMessageAndAck(bridge, messageSource).msg.msg should be(messageSource.desiredCommitThreshold)
      messageSource.getUncommittedCount should be(1)
      bridge.outstandingAcks.size() should be(1)
    }
    it("should process nacks") {
      val messageSource = new TestAsyncBatchAckingMessageSourceImpl(bridge, generator)
      // nack the first one
      fetchMessageAndAck(Ack.NACK, bridge, messageSource).msg.msg should be(0)
      // ack the rest up to the commit threshold
      for (i <- 1 until messageSource.desiredCommitThreshold) {
        val response = fetchMessageAndAck(bridge, messageSource)
        response.delay should be(0)
        response.msg.msg should be(i)
      }
      bridge.outstandingAcks.size() should be(messageSource.desiredCommitThreshold)
      // trigger commit
      fetchMessageAndAck(bridge, messageSource).msg.msg should be(messageSource.desiredCommitThreshold)
      messageSource.getUncommittedCount should be(1)
      bridge.outstandingAcks.size() should be(1)
      // failurehandler should have been called with the nack'ed message
      failures.size should be(1)
      failures.head.msg should be(0)
    }
  }
  describe("when timing out") {
    it("should respect the hard commit threshold") {
      val messageSource = new TestAsyncBatchAckingMessageSourceImpl(bridge, generator)
      val subAckTimeoutDelay = messageSource.ackTimeout - messageSource.ackTimeout / 10
      for (i <- 0 until messageSource.hardCommitThreshold) {
        val response = fetchMessage(bridge, messageSource, Some(subAckTimeoutDelay))
        response.delay should be(0)
        response.msg.msg should be(i)
      }
      // reached hardCommit, not handing out messages
      an [java.util.concurrent.TimeoutException] should be thrownBy fetchMessage(bridge, messageSource, Some(subAckTimeoutDelay))
      Thread.sleep((messageSource.ackTimeout / 10).toMillis)
      // exceeded ackTimeout, trigger cleanup
      messageSource.processOnce()
      messageSource.getUncommittedCount should be(1)
      failures.size should be(messageSource.hardCommitThreshold)
    }
  }


  case class FetchMessageResponse(delay: Long, msg: Ack.AckableMessage[Int])
  def fetchMessageAndAck(bridge: AsyncAckingBridge[Int], source: TestAsyncBatchAckingMessageSourceImpl): FetchMessageResponse =
    fetchMessageAndAck(Ack.ACK, bridge, source)
  def fetchMessageAndAck(ack: AckType, bridge: AsyncAckingBridge[Int], source: TestAsyncBatchAckingMessageSourceImpl): FetchMessageResponse = {
    val response = fetchMessage(bridge, source)
    bridge.outstandingAcks.put(Ack(response.msg.id, ack))
    response
  }
  def fetchMessage(bridge: AsyncAckingBridge[Int], source: TestAsyncBatchAckingMessageSourceImpl, await: Option[FiniteDuration] = None): FetchMessageResponse = {
    bridge.syncPoint.isEmpty should be(true)
    val request = Future {
      //println("about to take")
      val msg = bridge.syncPoint.take()
      //println("finished take")
      msg
    }
    bridge.syncPoint.isEmpty should be(true)
    Thread.sleep(20)  // give the Future a chance to get assigned to a thread and execute
    val delay = source.processOnce()
    FetchMessageResponse(delay, Await.result(request, await.getOrElse(source.ackTimeout + 100.millis)))
  }
}
