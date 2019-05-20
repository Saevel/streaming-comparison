package prv.saevel.streaming.comparison.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import prv.saevel.streaming.comparison.common.model.{OriginalUser, User}
import prv.saevel.streaming.comparison.common.utils.{JsonFormats, StreamProcessor}

object FlinkUserTransformation extends StreamProcessor[FlinkConfiguration, StreamExecutionEnvironment] with JsonFormats with FlinkKafkaIO {

  import org.apache.flink.streaming.api.scala._
  import spray.json._

  private var consumerAttempt: Option[FlinkKafkaConsumer[String]] = None

  private var producerAttempt: Option[FlinkKafkaProducer[String]] = None

  override def startStream(config: FlinkConfiguration)(implicit context: StreamExecutionEnvironment): Unit = {
    consumerAttempt = Option(kafkaConsumer(config.kafka, config.kafka.originalUsersTopic, "FlinkUserTransformation"))
    producerAttempt = Option(kafkaProducer(config.kafka, config.kafka.usersOutputTopic))

    consumerAttempt.foreach(consumer =>
      producerAttempt.foreach(producer =>
        context
          .addSource(consumer)
          .map(_.toJson.convertTo[OriginalUser])
          .map(originalUser => User(originalUser.id, originalUser.address, originalUser.country))
          .map(_.toJson.toString)
          .addSink(producer)
      )
    )
  }

  override def stopStream(implicit context: StreamExecutionEnvironment): Unit = {
    consumerAttempt.foreach(_.close)
    producerAttempt.foreach(_.close)
  }
}
