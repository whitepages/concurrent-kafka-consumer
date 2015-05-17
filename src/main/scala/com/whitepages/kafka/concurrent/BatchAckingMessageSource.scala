package com.whitepages.kafka.concurrent

import java.util.concurrent.{LinkedBlockingQueue, LinkedTransferQueue}

import com.whitepages.kafka.concurrent.Ack._
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.collection.JavaConverters._
import scala.util.{Random, Success, Failure, Try}

trait BatchAckingMessageSource[T] {
  def nextBatch(): Seq[T]
  def commit(): Unit
  // def commitUpTo(highWater: T): Unit    // TODO: Coming in future kafka versions
}

trait AsyncAckingBridge[T] {
  val syncPoint: LinkedTransferQueue[AckableMessage[T]] = new LinkedTransferQueue[AckableMessage[T]]()
  val outstandingAcks: LinkedBlockingQueue[Acknowledgement] = new LinkedBlockingQueue[Acknowledgement]()
  val failureHandler: (List[AckedMessage[T]]) => Unit
}
class HandlingAsyncAckingBridge[T](override val failureHandler: (List[AckedMessage[T]]) => Unit) extends AsyncAckingBridge[T]

trait AsyncBatchAckingMessageSource[T] extends StopableRunnable {
  val asyncBridge: AsyncAckingBridge[T]
  val messageSource: BatchAckingMessageSource[T]
  val desiredCommitThreshold: Int
  val ackTimeout: FiniteDuration
  val maxProcessDelay = 1000 // millis

  // These are lazy because they are referencing an abstract val at initialization otherwise
  // stop handing out messages if we exceed the hard threshold of uncommitted messages
  lazy val hardCommitThreshold = desiredCommitThreshold * 2
  // if we've been idle this long, might as well try to do the housekeeping.
  lazy val opportunisticCommitIdleDelay = ackTimeout

  val outstandingMessages = new mutable.HashMap[Long, AckableMessage[T]]()
  val failedMessages = new mutable.Queue[AckedMessage[T]]()

  // if we've requested messages, but find we don't have anyone to give them to, stash them here
  // until we can hand them out to the next person to ask. This can block committing indefinitely.
  var msgOnOffer: Seq[T] = Seq()
  // keep an eye on whether we're idle, so we can commit when we have nothing better to do.
  var lastActivity = System.currentTimeMillis()
  // how far out on the ice are we?
  private var uncommittedCount: Int = 0
  def getUncommittedCount = uncommittedCount

  private def okToCommit = msgOnOffer.isEmpty && outstandingMessages.isEmpty && uncommittedCount > 0

  private def isBored = uncommittedCount > 0 && noRecentActivity

  private def noRecentActivity = (System.currentTimeMillis() - lastActivity) > opportunisticCommitIdleDelay.toMillis

  private def doHousekeeping(): Unit = {
    // gather acks seen so far
    val processingAcks = mutable.ListBuffer[Acknowledgement]()
    asyncBridge.outstandingAcks.drainTo(processingAcks.asJava)

    // remove acks from outstandingMessages
    for {
      ack <- processingAcks
      msg <- outstandingMessages.remove(ack.id)
    } ack.ackType match {
      case ACK => Unit
      case unsuccessfulAckCode =>
        failedMessages.enqueue(AckedMessage(unsuccessfulAckCode, msg.msg))
    }

    // remove outstanding messages whose acks have expired
    // TODO: If we gave outstandingMessages an ordering, we would be able to shortcut the loop
    val expiredThreshold = System.currentTimeMillis() - ackTimeout.toMillis
    outstandingMessages.foreach {
      case (id, ackableMsg) =>
        if (ackableMsg.timestamp < expiredThreshold) {
          // return Cancellable futures and notify
          failedMessages.enqueue(AckedMessage(Ack.TIMEOUT, ackableMsg.msg))
          outstandingMessages.remove(id)
        }
    }

    // commit if possible
    if (okToCommit) {
      if (failedMessages.nonEmpty) {
        // Block this thread's execution until the failureHandler completes.
        // Exceptions during failureHandler execution escalate, and terminates all message processing.
        asyncBridge.failureHandler(failedMessages.toList)
      }
      messageSource.commit()
      uncommittedCount = 0
      failedMessages.clear()
    }
  }


  override def processOnce(): Long = {
    //println(s"looping. uncommited: $uncommittedCount, failed msgs: ${failedMessages.size}, acks unprocessed: ${asyncBridge.outstandingAcks.size()}, outstanding: ${outstandingMessages.size}, msgOnOffer: $msgOnOffer")
    if (uncommittedCount >= desiredCommitThreshold || isBored) doHousekeeping()

    // if we've exceeded the hard-commit threshold, stop giving out messages until we can commit,
    // which may not happen until all the outstanding messages time out
    if (uncommittedCount < hardCommitThreshold || msgOnOffer.nonEmpty) {
      // Checking hasWaitingConsumer cuts down on the cases where msgOnOffer is used.
      // If the other end only uses syncPoint.take, and the messageSource.nextBatch always returns a batch of size 1, msgOnOffer should always be empty.
      // TODO: Performance of hasWaitingConsumer? Implied to be good, but untested.
      if (asyncBridge.syncPoint.hasWaitingConsumer) {
        //println("has waiting consumer")
        if (msgOnOffer.isEmpty) {
          msgOnOffer = messageSource.nextBatch()
          uncommittedCount = uncommittedCount + msgOnOffer.size
        }
        if (msgOnOffer.nonEmpty) {
          val msgId = Random.nextLong()
          val nextMsg = AckableMessage[T](msgId, msgOnOffer.head, System.currentTimeMillis())
          //println("Attempting transfer")
          if (asyncBridge.syncPoint.tryTransfer(nextMsg)) {
            //println("Transfer succeeded")
            lastActivity = System.currentTimeMillis()
            outstandingMessages += Tuple2(msgId, nextMsg)
            msgOnOffer = msgOnOffer.tail
          }
        }

        0  // we're working happily, process again asap
      }
      else {
        //println("no waiting consumer")
        Math.min(maxProcessDelay, System.currentTimeMillis() - lastActivity) // nobody around, let's not spin-loop *too* fast.
      }

    }
    else 0  // we've exceeded our hard-commit threshold

  }
}

abstract class AsyncBatchAckingMessageSourceImpl[T](
  override val ackTimeout: FiniteDuration,
  override val asyncBridge: AsyncAckingBridge[T],
  override val desiredCommitThreshold: Int,
  override val messageSource: BatchAckingMessageSource[T]
) extends AsyncBatchAckingMessageSource[T]