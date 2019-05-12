package prv.saevel.streaming.comparison.flink

import java.time.Duration

import cats.data.ValidatedNel
import com.typesafe.config.Config
import prv.saevel.streaming.comparison.common.config.{BasicConfig, KafkaConfiguration}
import prv.saevel.streaming.comparison.common.utils.ConfigurationHelper

case class FlinkConfiguration(kafka: KafkaConfiguration, joinDuration: Duration, applicationName: String) extends BasicConfig

object FlinkConfiguration extends ConfigurationHelper {

  def apply(config: Config): ValidatedNel[Throwable, FlinkConfiguration] = for {
    kafkaSubconfig <- config.validatedConfig("kafka")
    joinDuration <- config.validatedDuration("join.duration")
    appName <- config.validatedString("application.name")
    kafkaConfiguration <- KafkaConfiguration(kafkaSubconfig)
  } yield FlinkConfiguration(kafkaConfiguration, joinDuration, appName)
}
