package prv.saevel.streaming.comparison.akka.streams

import java.time.Duration

import cats.data.ValidatedNel
import com.typesafe.config.Config
import prv.saevel.streaming.comparison.common.config.{BasicConfig, KafkaConfiguration}
import prv.saevel.streaming.comparison.common.utils.ConfigurationHelper

case class AkkaStreamsConfiguration(kafka: KafkaConfiguration,
                                    joinDuration: Duration,
                                    applicationName: String,
                                    other: Config) extends BasicConfig

object AkkaStreamsConfiguration extends ConfigurationHelper {

  def apply(config: Config): ValidatedNel[Throwable, AkkaStreamsConfiguration] = for {
    joinDuration <- config.validatedDuration("join.duration")
    appName <- config.validatedString("application.name")
    kafkaSubconfig <- config.validatedConfig("kafka")
    kafkaConfiguration <- KafkaConfiguration(kafkaSubconfig)
  } yield AkkaStreamsConfiguration(kafkaConfiguration, joinDuration, appName, config)
}
