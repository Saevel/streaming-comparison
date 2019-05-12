package prv.saevel.streaming.comparison.kafka.streams

import java.time.Duration
import java.util.Properties

import cats.data.ValidatedNel
import com.typesafe.config.Config
import org.apache.kafka.streams.StreamsConfig
import prv.saevel.streaming.comparison.common.config.{BasicConfig, KafkaConfiguration}
import prv.saevel.streaming.comparison.common.utils.ConfigurationHelper

case class KafkaStreamsConfiguration(kafka: KafkaConfiguration, joinDuration: Duration, applicationName: String) extends BasicConfig

object KafkaStreamsConfiguration extends ConfigurationHelper {

  def apply(config: Config): ValidatedNel[Throwable, KafkaStreamsConfiguration] = for {
    kafkaSubconfig <- config.validatedConfig("kafka")
    joinDuration <- config.validatedDuration("join.duration")
    applicationName <- config.validatedString("application.name")
    kafkaConfig <- KafkaConfiguration(kafkaSubconfig)
  } yield KafkaStreamsConfiguration(kafkaConfig, joinDuration, applicationName)

  implicit class KStreamsConfigToProperties(private val config: KafkaStreamsConfiguration) {
    def toProperties: Properties = {
      val props = new Properties()
      props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationName)
      props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.bootstrapServers)
      props
    }
  }
}
