package prv.saevel.streaming.comparison.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import prv.saevel.streaming.comparison.common.config.KafkaConfiguration
import prv.saevel.streaming.comparison.common.model.{OriginalUser, User}
import prv.saevel.streaming.comparison.common.utils.{JsonFormats, StreamProcessor}

object FlinkUserTransformation extends StreamProcessor[FlinkConfiguration, StreamExecutionEnvironment] with JsonFormats {

  import org.apache.flink.streaming.api.scala._
  import spray.json._

  override def startStream(config: FlinkConfiguration)(implicit context: StreamExecutionEnvironment): Unit = {
    context
      .addSource(consumer(config.kafka))
      .map(_.toJson.convertTo[OriginalUser])
      .map(originalUser => User(originalUser.id, originalUser.address, originalUser.country))
      .map(_.toJson.toString)
      .addSink(producer(config.kafka))
  }

  override def stopStream(implicit context: StreamExecutionEnvironment): Unit = {}

  private def producer(config: KafkaConfiguration): FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](
    config.bootstrapServers, config.originalUsersTopic, new SimpleStringSchema()
  )

  private def consumer(config: KafkaConfiguration): FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](
    config.originalUsersTopic, new SimpleStringSchema(), consumerProperties(config)
  )

  private def consumerProperties(config: KafkaConfiguration): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", config.bootstrapServers)
    props.put("auto.offset.reset", "earliest")
    props.put("group.id", "flinkUserTransformation")
    props
  }
}
