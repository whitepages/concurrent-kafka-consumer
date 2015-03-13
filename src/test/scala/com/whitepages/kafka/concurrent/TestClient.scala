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
import scala.collection.JavaConverters._


class TestClient extends FunSpec with BeforeAndAfterAll with BeforeAndAfterEach with Matchers {
  private[this] val kafka: MockKafka = new MockKafka()
  val testTopic = "testTopic"
  val messageCount = 1000   // needs to be even, at the least.
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
      val timeoutPct = 0
      val consumer = BuggyConsumer(client, failurePct, timeoutPct)

      val running = consumer.consumeInParallel(messageCount)
      val results = Await.result(running, 10.seconds)
      Thread.sleep(client.timeout.toMillis + 200)

      consumer.nacked.size + consumer.acked.size should be(messageCount)
      failures.size() should be((messageCount * (failurePct / 100.0)).toInt +- messageCount / 10)
      failures.size() should be(consumer.nacked.size())
      client.shutdown()
    }

    it("should handle multi-threaded nacks with timeouts") {
      val client = new ClientImpl(kafka.zkConnect, testTopic, "multithreadedTimeoutGroup", messageCount / 4)
        .start((l) => l.foreach(failures.add))

      val failurePct = 10
      val timeoutPct = 10
      val consumer = BuggyConsumer(client, failurePct, timeoutPct)

      val running = consumer.consumeInParallel(messageCount)
      val results = Await.result(running, (messageCount / client.hardCommitThreshold) * client.timeout + 1.second)
      val timeouts = results.filter(_._2 == Ack.TIMEOUT).map(_._1)
      val nacks = results.filter(_._2 == Ack.NACK).map(_._1)
      val acks = results.filter(_._2 == Ack.ACK).map(_._1)
      acks.size + timeouts.size + nacks.size should be(results.size) // this really just tests the BuggyConsumer
      Thread.sleep(client.timeout.toMillis + 200)

      consumer.timeout.size + consumer.nacked.size + consumer.acked.size should be(messageCount)
      failures.size() should be((messageCount * ((failurePct + timeoutPct) / 100.0)).toInt +- messageCount / 10)
      failures.size() should be(consumer.nacked.size() + consumer.timeout.size())

      timeouts.forall( ackable => failures.asScala.exists(_.msg.key() == ackable.msg.key() ) )
      nacks.forall( ackable => failures.asScala.exists(_.msg.key() == ackable.msg.key()) )

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

