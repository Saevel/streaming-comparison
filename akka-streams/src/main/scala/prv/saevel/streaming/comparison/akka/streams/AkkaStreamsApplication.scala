package prv.saevel.streaming.comparison.akka.streams

import cats.data.Validated.{Invalid, Valid}
import cats.data.ValidatedNel
import prv.saevel.streaming.comparison.common.utils.ExternalConfiguration

object AkkaStreamsApplication extends App with ExternalConfiguration with AkkaStreamsContextProvider {

  private val configuration: ValidatedNel[Throwable, AkkaStreamsConfiguration] = for {
    rawConf <- rawConfig
    conf <- AkkaStreamsConfiguration(rawConf)
  } yield conf

  configuration match {
    case Valid(config) => withContext(config){ implicit context =>
      AkkaStreamsUserTransformation.startStream(config)
      // TODO: Run joins
      // TODO: Run aggregations
    }

    case Invalid(errorList) => errorList.toList.foreach(_.printStackTrace)
  }
}
