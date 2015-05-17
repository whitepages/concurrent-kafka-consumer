package com.whitepages.kafka.concurrent.messagesource

import java.util.Properties

import com.whitepages.kafka.concurrent.BatchAckingMessageSource
import kafka.consumer.{ConsumerTimeoutException, ConsumerConfig}
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder, Decoder}

import scala.util.{Success, Failure, Try}

object KafkaAckingMessageSource {
  def byteConsumerSource(zk: String, topic: String, group: String, startAtEnd: Boolean = false): KafkaAckingMessageSource[String, Array[Byte]] = {
    val props = new Properties()
    props.put("zookeeper.connect", zk)
    props.put("group.id", group)
    if (startAtEnd)
      props.setProperty("auto.offset.reset", "largest")
    else
      props.setProperty("auto.offset.reset", "smallest")
    byteConsumerSource(props, topic)
  }
  def byteConsumerSource(props: Properties, topic: String): KafkaAckingMessageSource[String, Array[Byte]] = {
    new KafkaAckingMessageSource[String, Array[Byte]](props, topic, new StringDecoder(), new DefaultDecoder())
  }
}

class KafkaAckingMessageSource[K,V](consumerProps: Properties, topic: String, keyDecoder: Decoder[K], valueDecoder: Decoder[V])
  extends BatchAckingMessageSource[MessageAndMetadata[K, V]] {

  def overrideProps(props: Properties): Properties = {
    // Why would you use any of this with autocommit?
    props.setProperty("auto.commit.enable", "false")
    props
  }

  private val connector = kafka.consumer.Consumer.create(new ConsumerConfig(overrideProps(consumerProps)))
  private val iter = connector.createMessageStreams[K, V](Map(topic -> 1), keyDecoder, valueDecoder).get(topic).get.head.iterator()

  override def commit(): Unit = connector.commitOffsets

  // blocking call
  override def nextBatch(): Seq[MessageAndMetadata[K, V]] = {
    val n = Try(iter.next())
    n match {
      case Failure(e: ConsumerTimeoutException) => Seq()
      case Failure(e) => throw new RuntimeException("Unexpected exception from kafka", e)
      case Success(m) => Seq(m)
    }
  }
}
