package com.whitepages.kafka.concurrent.consumer

import com.whitepages.kafka.concurrent.Ack.{AckedMessage, Acknowledgement, AckableMessage}
import com.whitepages.kafka.concurrent._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent._

class AsyncKafkaConsumer[T](
                             val ackTimeout: FiniteDuration,
                             val desiredCommitThreshold: Int,
                             source: BatchAckingMessageSource[T],
                             failureHandler: (List[AckedMessage[T]]) => Unit)
  extends AsyncAckingConsumer[T] with BackgroundWorker {

  val bridge: AsyncAckingBridge[T] = new HandlingAsyncAckingBridge[T](failureHandler)

  def backgroundWorker(shutdownHook: () => Boolean) = {
    new AsyncBatchAckingMessageSourceImpl(ackTimeout, bridge, desiredCommitThreshold, source) {
      override def shuttingDown: () => Boolean = shutdownHook
    }
  }

  /**
   * Acks a message
   * @param ack Ack object containing info needed to ack a particular message
   */
  override def ack(ack: Acknowledgement): Unit = bridge.outstandingAcks.put(ack)

  /**
   * Get the next message from the kafka topic
   * @param ec implicit execution context used to create the future
   * @return Future containing an AckableMessage with the next message. This message should be acked at some
   *         point in the future by the caller
   */
  override def next(implicit ec: ExecutionContext): Future[AckableMessage[T]] = {
    if (workerException.isDefined) {
      // TODO: Future.failed?
      throw workerException.get
    }
    if (!running) {
      // TODO: Future.failed?
      throw new RuntimeException("Client worker is not running")
    }
    Future {
      //This can block, let the execution context take care of it
      //TODO: make default blocking execution context to provide to the users
      blocking {
        bridge.syncPoint.take
      }
    }
  }
}
