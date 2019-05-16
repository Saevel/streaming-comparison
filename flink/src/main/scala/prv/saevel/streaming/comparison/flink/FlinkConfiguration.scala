package prv.saevel.streaming.comparison.flink

import java.time.Duration

import cats.implicits._
import cats.data.ValidatedNel
import com.typesafe.config.Config
import prv.saevel.streaming.comparison.common.config.{BasicConfig, KafkaConfiguration}
import prv.saevel.streaming.comparison.common.utils.ConfigurationHelper

import scala.concurrent.duration.FiniteDuration

case class FlinkConfiguration(kafka: KafkaConfiguration, joinDuration: FiniteDuration, applicationName: String) extends BasicConfig

object FlinkConfiguration extends ConfigurationHelper {

  def apply(config: Config): ValidatedNel[Throwable, FlinkConfiguration] = (
    KafkaConfiguration(config.getConfig("kafka")),
    config.validatedDuration("join.duration"),
    config.validatedString("application.name")
  ).mapN(FlinkConfiguration.apply)
}
