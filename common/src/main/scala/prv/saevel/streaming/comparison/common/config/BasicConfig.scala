package prv.saevel.streaming.comparison.common.config

import java.time.Duration

trait BasicConfig {
  val kafka: KafkaConfiguration
  val joinDuration: Duration
  val applicationName: String
}
