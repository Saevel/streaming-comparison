package prv.saevel.streaming.comparison.kafka.streams

import java.time.Duration
import java.util.Properties

import cats.implicits._
import cats.data.ValidatedNel
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.StreamsConfig
import prv.saevel.streaming.comparison.common.config.{BasicConfig, KafkaConfiguration}
import prv.saevel.streaming.comparison.common.utils.ConfigurationHelper

case class KafkaStreamsConfiguration(kafka: KafkaConfiguration, joinDuration: Duration, applicationName: String) extends BasicConfig

object KafkaStreamsConfiguration extends ConfigurationHelper {

  def apply(config: Config): ValidatedNel[Throwable, KafkaStreamsConfiguration] = (
    KafkaConfiguration(config.getConfig("kafka")),
    config.validatedDuration("join.duration"),
    config.validatedString("application.name")
  ).mapN(KafkaStreamsConfiguration.apply)

  implicit class KStreamsConfigToProperties(private val config: KafkaStreamsConfiguration) {
    def toProperties: Properties = {
      val props = new Properties()
      props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationName)
      props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers)
      props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      props
    }
  }
}
