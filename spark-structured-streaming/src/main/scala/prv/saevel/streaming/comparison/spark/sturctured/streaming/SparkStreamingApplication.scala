package prv.saevel.streaming.comparison.spark.sturctured.streaming

import cats.data.Validated.Valid
import cats.data.ValidatedNel
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import prv.saevel.streaming.comparison.common.utils.ExternalConfiguration

object SparkStreamingApplication extends App with ExternalConfiguration {

  private val configuration: ValidatedNel[Throwable, SparkConfiguration] =
    SparkConfiguration(rawConfig.getOrElse(ConfigFactory.empty))

  configuration match {
    case Valid(sparkConfiguration) => {
      implicit val session = SparkSession
        .builder
        .appName(sparkConfiguration.applicationName)
        .config("spark.sql.streaming.checkpointLocation", sparkConfiguration.checkpointLocation)
        .getOrCreate

      SparkStreamingUserTransformation.startStream(sparkConfiguration)
      SparkStreamingBalanceVerification.startStream(sparkConfiguration)
      // TODO: Aggregations
    }
  }
}
