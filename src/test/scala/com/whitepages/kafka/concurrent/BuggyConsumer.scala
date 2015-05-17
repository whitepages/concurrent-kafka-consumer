package com.whitepages.kafka.concurrent

import java.util.concurrent.ConcurrentLinkedQueue
import com.whitepages.kafka.concurrent.Ack.{AckableMessage, AckType}

import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future
import scala.util.Random

case class BuggyConsumer[T](client: AsyncAckingConsumer[T], failRate: Int, timeoutRate: Int) {
  require(failRate >= 0 && failRate <= 100)
  require(timeoutRate >= 0 && timeoutRate <= 100)
  require(timeoutRate + failRate <= 100)

  val acked = new ConcurrentLinkedQueue[AckableMessage[T]]()
  val nacked = new ConcurrentLinkedQueue[AckableMessage[T]]()
  val timeout = new ConcurrentLinkedQueue[AckableMessage[T]]()

  def consumeInParallel(count: Int): Future[IndexedSeq[(AckableMessage[T], AckType)]] = {
    Future.sequence((1 to count).map( i => {
      client.next.map( msg => {
        val chance = Random.nextInt(100) + 1 // 1 to 100
        //Thread.sleep(Random.nextInt(100))
        if (chance <= failRate) {
          client.ack(Ack(msg.id, Ack.NACK))
          nacked.add(msg)
          (msg, Ack.NACK)
        }
        else if (chance > failRate + timeoutRate) {
          client.ack(Ack(msg.id, Ack.ACK))
          acked.add(msg)
          (msg, Ack.ACK)
        }
        else {
          timeout.add(msg) // timeout, no ack
          (msg, Ack.TIMEOUT)
        }
      })
    }))
  }
}
