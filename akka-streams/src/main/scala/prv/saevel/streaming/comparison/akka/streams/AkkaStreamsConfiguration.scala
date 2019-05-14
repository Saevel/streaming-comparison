package prv.saevel.streaming.comparison.akka.streams

import java.time.Duration

import cats.implicits._
import cats.data.ValidatedNel
import com.typesafe.config.Config
import prv.saevel.streaming.comparison.common.config.{BasicConfig, KafkaConfiguration}
import prv.saevel.streaming.comparison.common.utils.ConfigurationHelper

case class AkkaStreamsConfiguration(kafka: KafkaConfiguration,
                                    joinDuration: Duration,
                                    applicationName: String,
                                    other: Config) extends BasicConfig

object AkkaStreamsConfiguration extends ConfigurationHelper {

  def apply(config: => Config): ValidatedNel[Throwable, AkkaStreamsConfiguration] = (
    KafkaConfiguration(config.getConfig("kafka")),
    config.validatedDuration("join.duration"),
    config.validatedString("application.name")
  ).mapN(AkkaStreamsConfiguration(_, _, _, config))
}
