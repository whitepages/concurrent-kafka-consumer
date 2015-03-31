package com.whitepages.kafka.concurrent

object Ack {
  type AckType = Int
  val ACK: AckType = 1
  val NACK: AckType = ACK + 1
  val RETRY: AckType = NACK + 1       // duration? durability semantic? retry count?
  val TIMEOUT: AckType = RETRY + 1
  private val AckTypeList = List(ACK, NACK, RETRY, TIMEOUT)

  assert(AckTypeList.distinct.size == AckTypeList.size, "All AckTypes are unique")

  case class Acknowledgement(id: Long, ackType: AckType)
  def apply(id: Long, ackType: AckType) = new Acknowledgement(id, ackType)
}

