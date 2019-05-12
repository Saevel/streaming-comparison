package prv.saevel.streaming.comparison.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import prv.saevel.streaming.comparison.common.config.KafkaConfiguration
import prv.saevel.streaming.comparison.common.model.{OriginalUser, User}
import prv.saevel.streaming.comparison.common.utils.StreamProcessor

class FlinkUserTransformation extends StreamProcessor[FlinkConfiguration, StreamExecutionEnvironment] {

  import org.apache.flink.streaming.api.scala._

  override def startStream(config: FlinkConfiguration)(implicit context: StreamExecutionEnvironment): Unit = {
    context
      .addSource(consumer(config.kafka))
      .map(originalUser => User(originalUser.id, originalUser.address, originalUser.country))
      .addSink(producer(config.kafka))
  }

  // TODO: Can a Flink stream can be stopped?
  override def stopStream(implicit context: StreamExecutionEnvironment): Unit = {}

  private def producer(config: KafkaConfiguration): FlinkKafkaProducer[User] = ???

  private def consumer(config: KafkaConfiguration): FlinkKafkaConsumer[OriginalUser] = ???
}
