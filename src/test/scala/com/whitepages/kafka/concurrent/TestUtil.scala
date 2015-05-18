package com.whitepages.kafka.concurrent

import java.util.Properties

import kafka.producer.{ProducerConfig, KeyedMessage}
import kafka.server.{RunningAsController, RunningAsBroker, KafkaConfig, KafkaServer}
import kafka.utils.{ZkUtils, ZKStringSerializer, TestUtils, TestZKUtils}
import kafka.zk.EmbeddedZookeeper
import org.I0Itec.zkclient.ZkClient

trait MockZk {
  val zkConnect: String = TestZKUtils.zookeeperConnect
  var zookeeper: EmbeddedZookeeper = null
  var zkClient: ZkClient = null
  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  def start() {
    zookeeper = new EmbeddedZookeeper(zkConnect)
    TestUtils.waitUntilTrue(zookeeper.zookeeper.isRunning, "Zookeeper failed to start", 10000L)

    zkClient = new ZkClient(zookeeper.connectString, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    ZkUtils.setupCommonPaths(zkClient)
  }
}

class MockKafka extends MockZk {

  private[this] val brokerCount = 1
  private[this] var servers: List[KafkaServer] = List()

  private[this] val configs =
    for (props <- TestUtils.createBrokerConfigs(brokerCount, enableControlledShutdown = false))
    yield new KafkaConfig(props)

  val brokerList = TestUtils.getBrokerListStrFromConfigs(configs.toSeq)

  val numPartitions = 1
  val replicationFactor = 1

  override def start(): Unit = {
    super.start()

    servers = configs map { c: KafkaConfig => TestUtils.createServer(c) }
    TestUtils.waitUntilTrue( () => { servers.forall {
      (s) => { Set(RunningAsBroker.state, RunningAsController.state).contains(s.brokerState.currentState) }
    } }, "One or more brokers failed to start", 60000L)
  }

  def stop(): Unit = {
    servers map { s: KafkaServer => s.shutdown() }
    zookeeper.shutdown()
  }

  def createTopic(topic: String) = TestUtils.createTopic(zkClient, topic, numPartitions, replicationFactor, servers = servers)

}

class TestProducer(topic: String, brokers: String) {
  def properties: Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("request.required.acks", "-1")
    props.put("producer.type", "async")
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
    props.put("key.serializer.class", "kafka.serializer.StringEncoder")
    props
  }
  private lazy val config = new ProducerConfig(properties)
  private lazy val connector = new kafka.producer.Producer[String, Array[Byte]](config)

  def send(msgs: IndexedSeq[KeyedMessage[String, Array[Byte]]]) = {
    internalSend(msgs.map { (m) => new KeyedMessage[String, Array[Byte]](topic, m.key, m.message) })
  }
  private def internalSend(messages: Seq[KeyedMessage[String, Array[Byte]]]): Unit = connector.send(messages: _*)
}
