package com.whitepages.kafka.concurrent

import com.whitepages.kafka.consumer.ByteConsumer
import com.whitepages.kafka.consumer.Consumer.Settings
import com.whitepages.kafka.producer.ByteProducer
import kafka.server.{RunningAsController, RunningAsBroker, KafkaConfig, KafkaServer}
import kafka.utils.{ZkUtils, ZKStringSerializer, TestUtils, TestZKUtils}
import kafka.zk.EmbeddedZookeeper
import org.I0Itec.zkclient.ZkClient
import org.slf4j.{LoggerFactory, Logger}

import scala.util.Random

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

class TestConsumer(zk: String, fromTopic: String, startAtEnd: Boolean = false) extends ByteConsumer {
  override def logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def settings: Settings = Settings(zk, "test-consumer-group-" + Random.nextInt, fromTopic, autoCommit = false)

  override def defaultProperties = {
    val defaults = super.defaultProperties

    if (startAtEnd) defaults.setProperty("auto.offset.reset", "largest")
    defaults.setProperty("consumer.timeout.ms", "3000")
    defaults
  }
}
class TestProducer(topic: String, brokers: String)
  extends ByteProducer(topic, brokers) {

  override def defaultProperties = {
    val defaults = super.defaultProperties

    defaults.setProperty("producer.type", "sync")
    defaults.setProperty("partitioner.class", "kafka.producer.DefaultPartitioner")
    defaults.setProperty("key.serializer.class", "kafka.serializer.StringEncoder")
    defaults
  }
}
