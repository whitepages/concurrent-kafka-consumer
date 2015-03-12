package com.whitepages.kafka.concurrent

import java.util.concurrent.ConcurrentLinkedQueue
import com.whitepages.kafka.concurrent.Ack.AckType

import scala.concurrent.ExecutionContext.Implicits.global


import com.whitepages.kafka.concurrent.ClientImpl.{AckedMessage, AckableMessage}

import scala.concurrent.Future
import scala.util.Random

case class BuggyConsumer(client: ClientImpl, failRate: Int, timeoutRate: Int) {
  require(failRate >= 0 && failRate <= 100)
  require(timeoutRate >= 0 && timeoutRate <= 100)
  require(timeoutRate + failRate <= 100)

  val acked = new ConcurrentLinkedQueue[AckableMessage]()
  val nacked = new ConcurrentLinkedQueue[AckableMessage]()
  val timeout = new ConcurrentLinkedQueue[AckableMessage]()

  def consumeInParallel(count: Int): Future[IndexedSeq[(AckableMessage, AckType)]] = {
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
