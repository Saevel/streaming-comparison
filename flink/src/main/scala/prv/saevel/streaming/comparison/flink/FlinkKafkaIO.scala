package prv.saevel.streaming.comparison.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import prv.saevel.streaming.comparison.common.config.KafkaConfiguration

trait FlinkKafkaIO {

  protected def kafkaProducer(config: KafkaConfiguration, topic: String): FlinkKafkaProducer[String] =
    new FlinkKafkaProducer[String](config.bootstrapServers, topic, new SimpleStringSchema())

  protected def kafkaConsumer(config: KafkaConfiguration, topic: String, groupId: String): FlinkKafkaConsumer[String] =
    new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), consumerProperties(config, groupId))

  protected def consumerProperties(config: KafkaConfiguration, groupId: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", config.bootstrapServers)
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", groupId)
    props
  }
}
