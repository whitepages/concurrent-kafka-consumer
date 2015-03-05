package com.whitepages.kafka.concurrent

import kafka.message.MessageAndMetadata
import kafka.producer.KeyedMessage
import org.scalatest.{FunSpec, BeforeAndAfterAll, Matchers}
import scala.StringBuilder
import scala.collection.mutable
import scala.concurrent._
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class TestClient extends FunSpec with BeforeAndAfterAll with Matchers {
  private[this] val kafka: MockKafka = new MockKafka()
  val testTopic = "testTopic"
  val messageCount = 1000
  val messages = (0 to messageCount).map( _ => randomMessage )
  private[this] lazy val producer = new TestProducer(testTopic, kafka.brokerList)

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

  val failures = mutable.ListBuffer[MessageAndMetadata[String, Array[Byte]]]()


  describe("single-threaded consumer") {

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

    it("should explode if the failure handler explodes") {
      val client = new ClientImpl(kafka.zkConnect, testTopic, "anotherGroup", 1)
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

