package com.whitepages.kafka.concurrent

import java.util.concurrent.ConcurrentLinkedQueue

import com.whitepages.kafka.concurrent.Ack.AckedMessage
import com.whitepages.kafka.concurrent.consumer.AsyncKafkaConsumer
import com.whitepages.kafka.concurrent.messagesource.KafkaAckingMessageSource
import kafka.message.MessageAndMetadata
import kafka.producer.KeyedMessage
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, Matchers}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random


class TestAsyncKafkaConsumer extends FunSpec with BeforeAndAfterAll with BeforeAndAfterEach with Matchers {
  private[this] val kafka: MockKafka = new MockKafka()
  val testTopic = "testTopic"
  val messageCount = 1000
  // needs to be even, at the least.
  val messages = (0 to messageCount).map(_ => randomMessage)
  private[this] lazy val producer = new TestProducer(testTopic, kafka.brokerList)
  val failures = new ConcurrentLinkedQueue[AckedMessage[MessageAndMetadata[String,Array[Byte]]]]()

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
    val msgGroups = messages.grouped(messageCount / 10)
    msgGroups.foreach(msgGroup => producer.send(msgGroup))
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
      val source = KafkaAckingMessageSource.byteConsumerSource(kafka.zkConnect, testTopic, "someGroup")
      val client = new AsyncKafkaConsumer[MessageAndMetadata[String,Array[Byte]]](3.seconds, 100, source, (l) => l.foreach(failures.add)).start()
      val duration = 10.seconds
      (0 to messageCount).foreach(msgNum => {
        val msgF = client.next
        val msg = Await.result(msgF, duration)
        val expectedMsg = messages(msgNum).message
        msg.msg.message() should be(expectedMsg)
        client.ack(msg.id, Ack.ACK)
      })
      client.shutdown()
    }

    it("should handle multi-threaded processing") {
      val ackTimeout = 3.seconds
      val source = KafkaAckingMessageSource.byteConsumerSource(kafka.zkConnect, testTopic, "multithreadedGroup")
      val client = new AsyncKafkaConsumer[MessageAndMetadata[String,Array[Byte]]](ackTimeout, 100, source, (l) => l.foreach(failures.add)).start()

      val consumer = BuggyConsumer(client, 0, 0)

      val running = consumer.consumeInParallel(messageCount)
      Await.result(running, 10.seconds)
      Thread.sleep(ackTimeout.toMillis + 1000)

      failures.size() should be(0)
      consumer.acked.size() should be(messageCount)
      client.shutdown()
    }

    it("should handle multi-threaded nacks") {
      val ackTimeout = 3.seconds
      val source = KafkaAckingMessageSource.byteConsumerSource(kafka.zkConnect, testTopic, "multithreadedNackGroup")
      val client = new AsyncKafkaConsumer[MessageAndMetadata[String,Array[Byte]]](ackTimeout, 100, source, (l) => l.foreach(failures.add)).start()

      val failurePct = 15
      val timeoutPct = 0
      val consumer = BuggyConsumer(client, failurePct, timeoutPct)

      val running = consumer.consumeInParallel(messageCount)
      val results = Await.result(running, 10.seconds)
      Thread.sleep(ackTimeout.toMillis + 1000)

      consumer.nacked.size + consumer.acked.size should be(messageCount)
      failures.size() should be((messageCount * (failurePct / 100.0)).toInt +- messageCount / 10)
      failures.size() should be(consumer.nacked.size())
      client.shutdown()
    }

    it("should handle multi-threaded nacks with timeouts") {
      val ackTimeout = 3.seconds
      val source = KafkaAckingMessageSource.byteConsumerSource(kafka.zkConnect, testTopic, "multithreadedTimeoutGroup")
      val client = new AsyncKafkaConsumer[MessageAndMetadata[String,Array[Byte]]](ackTimeout, messageCount / 4, source, (l) => l.foreach(failures.add)).start()

      val failurePct = 10
      val timeoutPct = 10
      val consumer = BuggyConsumer(client, failurePct, timeoutPct)

      val running = consumer.consumeInParallel(messageCount)
      val results = Await.result(running, (messageCount / (client.desiredCommitThreshold * 2)) * ackTimeout + 1.second)
      val timeouts = results.filter(_._2 == Ack.TIMEOUT).map(_._1)
      val nacks = results.filter(_._2 == Ack.NACK).map(_._1)
      val acks = results.filter(_._2 == Ack.ACK).map(_._1)
      acks.size + timeouts.size + nacks.size should be(results.size) // this really just tests the BuggyConsumer
      Thread.sleep(ackTimeout.toMillis + 200)

      consumer.timeout.size + consumer.nacked.size + consumer.acked.size should be(messageCount)
      failures.size() should be((messageCount * ((failurePct + timeoutPct) / 100.0)).toInt +- messageCount / 10)
      failures.size() should be(consumer.nacked.size() + consumer.timeout.size())

      timeouts.forall(ackable => failures.asScala.exists(_.msg.key() == ackable.msg.key()))
      nacks.forall(ackable => failures.asScala.exists(_.msg.key() == ackable.msg.key()))

      client.shutdown()
    }

    it("should explode if the failure handler explodes") {
      val ackTimeout = 3.seconds
      val source = KafkaAckingMessageSource.byteConsumerSource(kafka.zkConnect, testTopic, "explosiveGroup")
      val client = new AsyncKafkaConsumer[MessageAndMetadata[String,Array[Byte]]](ackTimeout, 1, source, (l) => throw new RuntimeException()).start()

      val duration = 10.seconds
      client.ack(Await.result(client.next, duration).id, Ack.NACK)
      intercept[RuntimeException] {
        // one of the next two calls to next() should find the RuntimeException
        client.next.map(msg => client.ack(msg.id, Ack.NACK))
        Thread.sleep(ackTimeout.toMillis + 200)
        client.next
      }
    }


  }


}

