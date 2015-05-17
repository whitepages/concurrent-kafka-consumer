package com.whitepages.kafka.concurrent

import com.whitepages.kafka.concurrent.Ack.{Acknowledgement, AckableMessage}

import scala.concurrent.{Future, ExecutionContext}

trait AsyncAckingConsumer[T] {

  /**
   * Get the next message from the kafka topic
   * @param ec implicit execution context used to create the future
   * @return Future containing an AckableMessage with the next message. This message should be acked at some
   *         point in the future by the caller
   */
  def next(implicit ec: ExecutionContext): Future[AckableMessage[T]]

  /**
   * Ack a message with a particular id and type
   * @param id Id of message to Ack
   * @param ackType Type of ack this is (fail, ack, nack, etc.)
   */
  def ack(id: Long, ackType: Ack.AckType): Unit = ack(Ack(id, ackType))

  /**
   * Acks a message
   * @param ack Ack object containing info needed to ack a particular message
   */
  def ack(ack: Acknowledgement): Unit

}
