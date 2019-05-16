package prv.saevel.streaming.comparison.spark.sturctured.streaming

import java.time.Duration

import cats.implicits._
import com.typesafe.config.Config
import prv.saevel.streaming.comparison.common.config.{BasicConfig, KafkaConfiguration}
import prv.saevel.streaming.comparison.common.utils.ConfigurationHelper

case class SparkConfiguration(kafka: KafkaConfiguration,
                              joinDuration: Duration,
                              applicationName: String,
                              checkpointLocation: String,
                              watermark: Duration) extends BasicConfig

object SparkConfiguration extends ConfigurationHelper {

  def apply(config: => Config): ErrorOr[SparkConfiguration] = (
    KafkaConfiguration(config.getConfig("kafka")),
    config.validatedDuration("join.duration"),
    config.validatedString("application.name"),
    config.validatedString("checkpoint.location"),
    config.validatedDuration("watermark.duration")
    ).mapN(SparkConfiguration.apply)
}