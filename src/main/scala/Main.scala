import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import scala.collection.JavaConverters._
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}

/*
 Test instructions:
 $ JAR_NAME=kafka-consumer-offset-metadata-reproduce-assembly-0.1.0-SNAPSHOT.jar
 $ sbt assembly
 $ java -jar $JAR_NAME __consumer_offsets
 $ java -jar $JAR_NAME __consumer_offsets kafka-broker.service.consul:9094
 */

object KafkaMetadataReproduce {

  def main(args: Array[String]) {
    val topic = args(0)
    val externalBrokerAddress = if (args.isDefinedAt(1)) Some(args(1)) else None
    externalBrokerAddress match {
      case Some(brokerAddress) =>
        runConsumer(brokerAddress, topic)
      case None =>
        val kafkaConfig = EmbeddedKafkaConfig(kafkaPort = 6001, zooKeeperPort = 6000)
        EmbeddedKafka.start()(kafkaConfig)
        val brokerAddress = s"localhost:${kafkaConfig.kafkaPort}"
        runConsumer(brokerAddress, topic)
        EmbeddedKafka.stop()
    }
  }

  def runConsumer(brokerAddress: String, topic: String): Unit = {
    val id = "test.mikemintz.lag-tracker-reproduce"
    val options = Map(
      ConsumerConfig.CLIENT_ID_CONFIG -> id,
      ConsumerConfig.GROUP_ID_CONFIG -> id,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerAddress,
      ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG -> false,
      ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG -> 30000,
    ).asInstanceOf[Map[String, AnyRef]]
    val deserializer = new ByteArrayDeserializer
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](options.asJava, deserializer, deserializer)

    println(s"Listing partitions for $topic")
    val offsetsTopicPartitions = consumer
      .partitionsFor(topic)
      .asScala
      //.filter(_.partition == 26)
      .map { info =>
        new TopicPartition(info.topic, info.partition)
      }
      .asJava
    println(s"Found partitions: $offsetsTopicPartitions")

    println("Calling assign()")
    consumer.assign(offsetsTopicPartitions)

    println("Calling seekToEnd()")
    consumer.seekToEnd(offsetsTopicPartitions)

    println("Calling endOffsets() for the first time")
    consumer.endOffsets(offsetsTopicPartitions)

    println("Calling poll()")
    consumer.poll(1000L)

    println("Calling endOffsets() for the second time")
    consumer.endOffsets(offsetsTopicPartitions)

    println("Done")
  }
}
