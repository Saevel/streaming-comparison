package prv.saevel.streaming.comparison.flink

import cats.data.Validated.Valid
import cats.data.ValidatedNel
import com.typesafe.config.ConfigFactory
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import prv.saevel.streaming.comparison.common.utils.ExternalConfiguration

object FlinkApplication extends App with ExternalConfiguration {

  private val configuration: ValidatedNel[Throwable, FlinkConfiguration] =
    FlinkConfiguration(rawConfig.getOrElse(ConfigFactory.empty))

  configuration match {
    case Valid(config) => {
      implicit val env = StreamExecutionEnvironment.getExecutionEnvironment
      FlinkAccountBalanceVerification.startStream(config)
      FlinkUserTransformation.startStream(config)
      // TODO: Aggregations
    }
  }
}
