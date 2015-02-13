package com.whitepages.kafka.concurrent

import java.util.concurrent._

import scala.collection.JavaConverters._

import com.whitepages.kafka.consumer.ByteConsumer
import com.whitepages.kafka.consumer.Consumer.Settings
import kafka.consumer.ConsumerTimeoutException
import kafka.message.MessageAndMetadata
import org.slf4j.{LoggerFactory, Logger}
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

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


class ClientImpl(zk: String, topic: String, group: String, desiredCommitThreshold: Int = 100, startAtEnd: Boolean = false) {
  import Ack._

  type Msg = MessageAndMetadata[String, Array[Byte]]
  case class AckableMessage(id: Long, msg: Msg, timestamp: Long)
  case class AckedMessage(ackType: AckType, msg: Msg)

  val timeout = 3.second
  val hardCommitThreshold = desiredCommitThreshold * 2

  // these should be the ONLY pieces of /mutable/ shared data between this class and the inner thread
  private val syncPoint = new LinkedTransferQueue[AckableMessage]()
  private val outstandingAcks = new LinkedBlockingQueue[Acknowledgement]()
  private var shuttingDown = false

  private val t = new Thread( new Runnable {
    val consumer = new ByteConsumerImpl(zk, topic, group, startAtEnd)

    val outstandingMessages = new mutable.HashMap[Long, AckableMessage]()
    val failedMessages = new mutable.Queue[AckedMessage]()

    var msgOnOffer: Option[AckableMessage] = None
    def okToCommit = msgOnOffer.isEmpty && outstandingMessages.isEmpty && consumer.consumedCount > 0

    def run() {
      while (!shuttingDown) {

        // commit if we can, and we've crossed a threshold
        if (consumer.consumedCount > desiredCommitThreshold) {
          // gather acks seen so far
          val processingAcks = mutable.ListBuffer[Acknowledgement]()
          outstandingAcks.drainTo(processingAcks.asJava)

          // remove acks from outstandingMessages
          processingAcks.foreach((ack) => {
            val msgOpt = outstandingMessages.remove(ack.id)
            msgOpt.foreach(msg =>
              ack.ackType match {
                case ACK => Unit
                case unsuccessfulAckCode => failedMessages.enqueue(AckedMessage(unsuccessfulAckCode, msg.msg))
              }
            )
          })

          // remove outstanding messages whose acks have expired
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
            if (failedMessages.nonEmpty) failureHandler(failedMessages.toList)
            consumer.commit()
            failedMessages.clear()
          }
        }

        // if we've exceeded the hard-commit threshold, stop giving out messages until we can commit,
        // which may not happen until all the outstanding messages time out
        if (consumer.consumedCount < hardCommitThreshold || msgOnOffer.nonEmpty) {
          try {
            val nextMsg: AckableMessage = msgOnOffer.getOrElse(AckableMessage(Random.nextLong(), consumer.next(), 0))
            msgOnOffer = None
            if (!syncPoint.tryTransfer(nextMsg.copy(timestamp = System.currentTimeMillis()), 100, TimeUnit.MILLISECONDS))
              msgOnOffer = Some(nextMsg) // nobody around, see if we can do some other work and try again later
            else {
              outstandingMessages += Tuple2(nextMsg.id, nextMsg)
            }

          } catch {
            case e: ConsumerTimeoutException => Unit // No messages, try again later
          }
        }

      }
      // TODO: shut the consumer down?
    }
  })

  class UncaughtExceptionHandler extends Thread.UncaughtExceptionHandler {
    override def uncaughtException(t: Thread, e: Throwable): Unit = println("Uncaught exception: " + e)  // TODO:
  }


  def next: Future[AckableMessage] = {
    Future {
      syncPoint.take
    }
  }

  def ack(ack: Acknowledgement) = outstandingAcks.put(ack)

  def start(): ClientImpl = start(ignoreFailuresHandler)
  def start(handler: FailureHandler): ClientImpl = {
    require(!started || handler == failureHandler, "Can't change the failure handler after start")


    failureHandler = handler
    t.setUncaughtExceptionHandler(new UncaughtExceptionHandler())
    t.start()
    this
  }
  def started(): Boolean = t.isAlive
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
