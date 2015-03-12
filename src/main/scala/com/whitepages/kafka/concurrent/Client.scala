package com.whitepages.kafka.concurrent


import java.util.concurrent._

import com.whitepages.kafka.concurrent.Ack._
import com.whitepages.kafka.concurrent.ClientImpl.AckableMessage
import com.whitepages.kafka.consumer.ByteConsumer
import com.whitepages.kafka.consumer.Consumer.Settings
import kafka.consumer.ConsumerTimeoutException
import kafka.message.MessageAndMetadata
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random


class ByteConsumerImpl(zk: String, topic: String, group: String, startAtEnd: Boolean = false) extends ByteConsumer {
  val timeout = 1.second

  override def logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def settings: Settings = Settings(zk, group, topic, autoCommit = false)

  override def defaultProperties = {
    val defaults = super.defaultProperties
    if (startAtEnd)
      defaults.setProperty("auto.offset.reset", "largest")
    else
      defaults.setProperty("auto.offset.reset", "smallest")
    defaults.setProperty("consumer.timeout.ms", timeout.toMillis.toString)
    defaults
  }
}

object ClientImpl {
  type Msg = MessageAndMetadata[String, Array[Byte]]
  case class AckableMessage(id: Long, msg: Msg, timestamp: Long)
  case class AckedMessage(ackType: AckType, msg: Msg)
}


class ClientImpl(zk: String, topic: String, group: String, desiredCommitThreshold: Int = 100, startAtEnd: Boolean = false) {
  import ClientImpl._

  val timeout = 3.second
  val hardCommitThreshold = desiredCommitThreshold * 2
  val opportunisticCommitIdleDelay = timeout

  // these should be the ONLY pieces of /mutable/ shared data between this class and the inner thread
  private val syncPoint = new LinkedTransferQueue[AckableMessage]()
  private val outstandingAcks = new LinkedBlockingQueue[Acknowledgement]()
  private var shuttingDown = false
  private var workerException: Option[Throwable] = None

  private def newWorkerThread() = new Thread( new Runnable {
    val consumer = new ByteConsumerImpl(zk, topic, group, startAtEnd)

    val outstandingMessages = new mutable.HashMap[Long, AckableMessage]()
    val failedMessages = new mutable.Queue[AckedMessage]()

    var msgOnOffer: Option[AckableMessage] = None
    var lastActivity = System.currentTimeMillis()
    def okToCommit = msgOnOffer.isEmpty && outstandingMessages.isEmpty && consumer.consumedCount > 0

    def run() {
      while (!shuttingDown) {
//println(s"looping. consumedCount: ${consumer.consumedCount}, failed msgs: ${failedMessages.size}, acks unprocessed: ${outstandingAcks.size()}, outstanding: ${outstandingMessages.size}, msgOnOffer: $msgOnOffer")
        // do housekeeping (and possibly commit) if we've crossed a threshold of outstanding messages, or if we apparently don't have anything better to do
        if (consumer.consumedCount >= desiredCommitThreshold
            || (consumer.consumedCount > 0 && System.currentTimeMillis() - lastActivity > opportunisticCommitIdleDelay.toMillis)) {

          // gather acks seen so far
          val processingAcks = mutable.ListBuffer[Acknowledgement]()
          outstandingAcks.drainTo(processingAcks.asJava)

          // remove acks from outstandingMessages
          for { ack <- processingAcks
                msg <- outstandingMessages.remove(ack.id) } ack.ackType match {
            case ACK => Unit
            case unsuccessfulAckCode =>
              failedMessages.enqueue(AckedMessage(unsuccessfulAckCode, msg.msg))
          }

          // remove outstanding messages whose acks have expired
          // TODO: If we gave outstandingMessages an ordering, we would be able to shortcut the loop
          val expiredThreshold = System.currentTimeMillis() - timeout.toMillis
          outstandingMessages.foreach {
            case (id, ackableMsg) => {
              if (ackableMsg.timestamp < expiredThreshold) {
                failedMessages.enqueue(AckedMessage(Ack.TIMEOUT, ackableMsg.msg))
                outstandingMessages.remove(id)
              }
            }
          }

          // commit if possible
          if (okToCommit) {
            if (failedMessages.nonEmpty) {
              failureHandler(failedMessages.toList)
            }
            consumer.commit()
            failedMessages.clear()
          }
        }

        // if we've exceeded the hard-commit threshold, stop giving out messages until we can commit,
        // which may not happen until all the outstanding messages time out
        if (consumer.consumedCount <= hardCommitThreshold || msgOnOffer.nonEmpty) {
          // Cuts down on the cases where msgOnOffer is used.
          // Since we only use syncPoint.take, it might preclude the need for msgOnOffer entirely.
          // TODO: Performance of hasWaitingConsumer? Implied to be good, but untested.
          if (syncPoint.hasWaitingConsumer) {
            try {
              val nextMsg: AckableMessage = msgOnOffer.getOrElse(AckableMessage(Random.nextLong(), consumer.next(), 0)).copy(timestamp = System.currentTimeMillis())
              msgOnOffer = None
              if (!syncPoint.tryTransfer(nextMsg))
                msgOnOffer = Some(nextMsg) // nobody around, see if we can do some other work and try again later
              else {
                lastActivity = System.currentTimeMillis()
                outstandingMessages += Tuple2(nextMsg.id, nextMsg)
              }

            } catch {
              case e: ConsumerTimeoutException => Unit // No messages, try again later
            }
          }
          else {
            Thread.sleep(20) // nobody around, let's not spin-loop *too* fast.
          }
        }

      }
      // TODO: shut the consumer down?
    }
  })

  private val t: Thread = newWorkerThread()   // TODO: Attempt to replace it if it stops?

  class UncaughtExceptionHandler extends Thread.UncaughtExceptionHandler {
    override def uncaughtException(t: Thread, e: Throwable): Unit = {
      workerException = Some(e)
      //println("Uncaught exception: " + e)
    }
  }


  def next(implicit ec: ExecutionContext): Future[AckableMessage] = {
    if (workerException.isDefined) {
      throw workerException.get
    }
    if (!running) {
      throw new RuntimeException("Client worker is not running")
    }
    Future {
      syncPoint.take
    }
  }

  def ack(id: Long, ackType: Ack.AckType): Unit = ack(Ack(id, ackType))
  def ack(ack: Acknowledgement): Unit = outstandingAcks.put(ack)

  def start(): ClientImpl = start(ignoreFailuresHandler)
  def start(handler: FailureHandler): ClientImpl = {
    require(!running || handler == failureHandler, "Can't change the failure handler after start")

    failureHandler = handler
    t.setUncaughtExceptionHandler(new UncaughtExceptionHandler())
    t.start()
    this
  }
  def running(): Boolean = t.isAlive
  def shutdown() = {
    shuttingDown = true
    t.join()
    this
  }

  // The failure handler is a blocking method, executed by the inner thread just prior to committing offsets.
  //
  // Committing the outstanding messages cannot happen until this function finishes.
  // Giving out more messages cannot happen while this function is running.
  // Exceptions in this method are not caught, and will explode the client class
  // TODO: Are there other things the client might do (besides block) based on a return value from this function?
  type FailureHandler = (List[AckedMessage]) => Unit
  val ignoreFailuresHandler: FailureHandler = (failures) => Unit
  private var failureHandler = ignoreFailuresHandler // set in start(), should never be changed after that

}
