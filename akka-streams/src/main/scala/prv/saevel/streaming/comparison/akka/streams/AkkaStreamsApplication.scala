package prv.saevel.streaming.comparison.akka.streams

import cats.data.Validated.{Invalid, Valid}
import cats.data.ValidatedNel
import com.typesafe.config.ConfigFactory
import prv.saevel.streaming.comparison.common.utils.ExternalConfiguration

object AkkaStreamsApplication extends App with ExternalConfiguration with AkkaStreamsContextProvider {

  private val configuration: ValidatedNel[Throwable, AkkaStreamsConfiguration] =
    AkkaStreamsConfiguration(rawConfig.getOrElse(ConfigFactory.empty))

  configuration match {
    case Valid(config) => withContext(config){ implicit context =>
      AkkaStreamsUserTransformation.startStream(config)
      // TODO: Run joins
      // TODO: Run aggregations
    }

    case Invalid(errorList) => errorList.toList.foreach(_.printStackTrace)
  }
}