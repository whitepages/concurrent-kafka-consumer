package com.whitepages.kafka.concurrent

import java.util.concurrent.ConcurrentLinkedQueue

import com.whitepages.kafka.concurrent.ClientImpl.{AckedMessage, AckableMessage}
import kafka.message.MessageAndMetadata
import kafka.producer.KeyedMessage
import org.scalatest.{BeforeAndAfterEach, FunSpec, BeforeAndAfterAll, Matchers}
import scala.StringBuilder
import scala.collection.mutable
import scala.concurrent._
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class TestClient extends FunSpec with BeforeAndAfterAll with BeforeAndAfterEach with Matchers {
  private[this] val kafka: MockKafka = new MockKafka()
  val testTopic = "testTopic"
  val messageCount = 1000
  val messages = (0 to messageCount).map( _ => randomMessage )
  private[this] lazy val producer = new TestProducer(testTopic, kafka.brokerList)
  val failures = new ConcurrentLinkedQueue[AckedMessage]()

  def randomMessage: KeyedMessage[String, Array[Byte]] = {
    new KeyedMessage[String, Array[Byte]](testTopic, randomPrintableString(8), randomPrintableString(Random.nextInt(200) + 8).getBytes("UTF-8"))
  }
  def randomPrintableString(length: Int, sb: mutable.StringBuilder = new StringBuilder()): String = {
    if (length > 0)
      randomPrintableString(length - 1, sb + Random.nextPrintableChar())
    else
      sb.toString()
  }

  override def beforeAll() {
    kafka.start()
    kafka.createTopic(testTopic)
    val msgGroups = messages.grouped(messageCount/10)
    msgGroups.foreach( msgGroup => producer.send(msgGroup) )
  }
  override def afterAll() = {
    kafka.stop()
  }
  override def beforeEach() = {
    failures.clear()
  }



  describe("single-threaded consumer") {

    // Note: the group name needs to be distinct for each client instance here, but exotic characters cause problems.

    it("should consume single-threaded") {

      val client = new ClientImpl(kafka.zkConnect, testTopic, "someGroup").start()
      val duration = 10.seconds
      (0 to messageCount).foreach( msgNum => {
        //println(s"checking message $msgNum")
        val msgF = client.next
        val msg = Await.result(msgF, duration)
        val expectedMsg = messages(msgNum).message
        msg.msg.message() should be(expectedMsg)
        client.ack(msg.id, Ack.ACK)
      })
      client.shutdown()
    }

    it("should handle multi-threaded processing") {
      val client = new ClientImpl(kafka.zkConnect, testTopic, "multithreadedGroup")
        .start((l) => l.foreach(failures.add))

      val consumer = BuggyConsumer(client, 0, 0)

      val running = consumer.consumeInParallel(messageCount)
      Await.result(running, 10.seconds)
      Thread.sleep(client.timeout.toMillis + 200)

      failures.size() should be(0)
      consumer.acked.size() should be(messageCount)
      client.shutdown()
    }

    it("should handle multi-threaded nacks") {
      val client = new ClientImpl(kafka.zkConnect, testTopic, "multithreadedNackGroup")
        .start((l) => l.foreach(failures.add))

      val failurePct = 15
      val consumer = BuggyConsumer(client, failurePct, 0)

      val running = consumer.consumeInParallel(messageCount)
      val results = Await.result(running, 1000.seconds)
      Thread.sleep(client.timeout.toMillis + 200)

//      println("Acked: " + results.count(_._2 == Ack.ACK))
//      println("Nacked: " + results.count(_._2 == Ack.NACK))

      consumer.nacked.size + consumer.acked.size should be(messageCount)
      failures.size() should be((messageCount * (failurePct / 100.0)).toInt +- messageCount / 10)
      failures.size() should be(consumer.nacked.size())
      client.shutdown()
    }


    it("should explode if the failure handler explodes") {
      val client = new ClientImpl(kafka.zkConnect, testTopic, "explosiveGroup", 1)
        .start((l) => throw new RuntimeException())
      val duration = 10.seconds
      client.ack(Await.result(client.next, duration).id, Ack.NACK)
      intercept[RuntimeException] {
        // one of the next two calls to next() should find the RuntimeException
        client.next.map( msg => client.ack(msg.id, Ack.NACK))
        Thread.sleep(client.timeout.toMillis + 200)
        client.next

      }
    }



  }



}

