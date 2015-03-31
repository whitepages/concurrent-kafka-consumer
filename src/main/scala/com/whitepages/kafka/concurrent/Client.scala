package com.whitepages.kafka.concurrent


import java.util.concurrent._

import com.whitepages.kafka.concurrent.Ack._
import com.whitepages.kafka.consumer.ByteConsumer
import com.whitepages.kafka.consumer.Consumer.Settings
import kafka.consumer.ConsumerTimeoutException
import kafka.message.MessageAndMetadata
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.blocking
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

  // TODO: Are there other things the client might do (besides block) based on a return value from this function?
  val ignoreFailuresHandler: FailureHandler = (failures) => Unit
  type FailureHandler = (List[AckedMessage]) => Unit

  sealed trait FailureHandlerResponse

  case object Die extends FailureHandlerResponse

  case object Rerun extends FailureHandlerResponse

  case class Retry(msgs: Seq[Msg]) extends FailureHandlerResponse


  case class KafkaConnectionSettings(zk: String,
                                     topic: String,
                                     group: String,
                                     startAtEnd: Boolean = false)

  case class RunnableKafkaWorkerSharedState(syncPoint: LinkedTransferQueue[AckableMessage], // read/write
                                            outstandingAcks: LinkedBlockingQueue[Acknowledgement], // read/write
                                            shutdownIndicator: () => Boolean, // read
                                            failureHandler: () => FailureHandler) // read


  class RunnableKafkaWorker(kafkaConnectionSettings: KafkaConnectionSettings,
                            sharedState: RunnableKafkaWorkerSharedState,
                            desiredCommitThreshold: Int,
                            ackTimeout: FiniteDuration) extends Runnable {

    // stop handing out messages if we exceed the hard threshold of uncommitted messages
    val hardCommitThreshold = desiredCommitThreshold * 2
    // if we've been idle this long, might as well try to do the housekeeping.
    val opportunisticCommitIdleDelay = ackTimeout

    val consumer = new ByteConsumerImpl(
      kafkaConnectionSettings.zk,
      kafkaConnectionSettings.topic,
      kafkaConnectionSettings.group,
      kafkaConnectionSettings.startAtEnd
    )

    val outstandingMessages = new mutable.HashMap[Long, AckableMessage]()
    val failedMessages = new mutable.Queue[AckedMessage]()

    // if we request a message from kafka, but find we don't have anyone to give it to, stash it here
    // until we can hand it out to the next person to ask. This can block committing indefinitely, so it should be
    // a rare event that this is nonEmpty.
    var msgOnOffer: Option[AckableMessage] = None
    // keep an eye on whether we're idle, so we can commit when we have nothing better to do.
    var lastActivity = System.currentTimeMillis()

    private def okToCommit = msgOnOffer.isEmpty && outstandingMessages.isEmpty && consumer.consumedCount > 0

    private def isBored = consumer.consumedCount > 0 &&
      System.currentTimeMillis() - lastActivity > opportunisticCommitIdleDelay.toMillis

    override def run(): Unit = {
      while (!sharedState.shutdownIndicator()) {
        //println(s"looping. consumedCount: ${consumer.consumedCount}, failed msgs: ${failedMessages.size}, acks unprocessed: ${outstandingAcks.size()}, outstanding: ${outstandingMessages.size}, msgOnOffer: $msgOnOffer")

        if (consumer.consumedCount >= desiredCommitThreshold || isBored) doHousekeeping()

        // if we've exceeded the hard-commit threshold, stop giving out messages until we can commit,
        // which may not happen until all the outstanding messages time out
        if (consumer.consumedCount <= hardCommitThreshold || msgOnOffer.nonEmpty) {
          // Checking hasWaitingConsumer cuts down on the cases where msgOnOffer is used.
          // If the other end only uses syncPoint.take, msgOnOffer should never be nonEmpty.
          // TODO: Performance of hasWaitingConsumer? Implied to be good, but untested.
          if (sharedState.syncPoint.hasWaitingConsumer) {
            try {
              val nextMsg: AckableMessage = msgOnOffer.getOrElse(AckableMessage(Random.nextLong(), consumer.next(), 0)).copy(timestamp = System.currentTimeMillis())
              msgOnOffer = None
              if (!sharedState.syncPoint.tryTransfer(nextMsg))
              // Contrary to our previous understanding, there's nobody around,
              // so see if we can do some other work and try again later
                msgOnOffer = Some(nextMsg)
              else {
                lastActivity = System.currentTimeMillis()
                outstandingMessages += Tuple2(nextMsg.id, nextMsg)
              }

            } catch {
              case e: ConsumerTimeoutException => Unit // No messages, try again later
            }
          }
          else {
            Thread.sleep(50) // nobody around, let's not spin-loop *too* fast.
          }
        }

      }
      // TODO: shut the consumer down?
    }

    /**
     * Processes all acks seen so far, removes acks that have expired, enqueue failure messages if unsuccessful
     * acks are seen. Possibly commits if its 'okToCommit'
     */
    private def doHousekeeping(): Unit = {
      // gather acks seen so far
      val processingAcks = mutable.ListBuffer[Acknowledgement]()
      sharedState.outstandingAcks.drainTo(processingAcks.asJava)

      // remove acks from outstandingMessages
      for {
        ack <- processingAcks
        msg <- outstandingMessages.remove(ack.id)
      } yield ack.ackType match {
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
          sharedState.failureHandler()(failedMessages.toList)
        }
        consumer.commit()
        failedMessages.clear()
      }
    }
  }

}


class ClientImpl(zk: String, topic: String, group: String, val desiredCommitThreshold: Int = 100, startAtEnd: Boolean = false) {
  import ClientImpl._

  val timeout = 3.second // how long to wait for an Ack

  // these should be the ONLY pieces of /mutable/ shared data between this class and the RunnableKafkaWorker
  private val syncPoint = new LinkedTransferQueue[AckableMessage]()
  private val outstandingAcks = new LinkedBlockingQueue[Acknowledgement]()
  private var shuttingDown = false
  private var workerException: Option[Throwable] = None
  // The failure handler is a blocking method, executed by the RunnableKafkaWorker just prior to committing offsets.
  // This means:
  //   - Committing the outstanding messages cannot happen until this function finishes.
  //   - Giving out more messages cannot happen while this function is running.
  //   - Exceptions in this method are not caught, and will explode the RunnableKafkaWorker
  private var failureHandler = ignoreFailuresHandler // set in start(), should never be changed after that
  // TODO: Other types
  // retry (reliable, in-memory-is-good-enough, etc)
  // deadletter
  // etc

  private val sharedState = RunnableKafkaWorkerSharedState(
    syncPoint,
    outstandingAcks,
    () => shuttingDown,
    () => failureHandler
  )
  private val connectionSettings = KafkaConnectionSettings(zk, topic, group, startAtEnd)

  class UncaughtExceptionHandler extends Thread.UncaughtExceptionHandler {
    override def uncaughtException(t: Thread, e: Throwable): Unit = {
      workerException = workerException.orElse(Some(e))
      //println("Uncaught exception: " + e)
    }
  }

  // this manages the actual kafka client instance
  private def newWorkerThread() = {
    val t = new Thread(
      new RunnableKafkaWorker(connectionSettings, sharedState, desiredCommitThreshold, timeout)
    )
    t.setUncaughtExceptionHandler(new UncaughtExceptionHandler())
    t
  }

  private val t: Thread = newWorkerThread() // TODO: Attempt to replace it if it stops?

  // uses the caller's execution context to create a future message
  def next(implicit ec: ExecutionContext): Future[AckableMessage] = {
    if (workerException.isDefined) {
      throw workerException.get
    }
    if (!running) {
      throw new RuntimeException("Client worker is not running")
    }
    Future {
      //This can block, let the execution context take care of it
      //TODO: make default blocking execution context to provide to the users
      blocking { syncPoint.take }
    }
  }

  def ack(id: Long, ackType: Ack.AckType): Unit = ack(Ack(id, ackType))

  def ack(ack: Acknowledgement): Unit = outstandingAcks.put(ack)

  def start(): ClientImpl = start(ignoreFailuresHandler)

  def start(handler: FailureHandler): ClientImpl = {
    require(!running || handler == failureHandler, "Can't change the failure handler after start")

    failureHandler = handler
    t.start()
    this
  }

  def running(): Boolean = t.isAlive

  def shutdown() = {
    shuttingDown = true
    t.join()
    this
  }

}
