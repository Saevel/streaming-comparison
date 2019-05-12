package prv.saevel.streaming.comparison.common.utils

import java.time.Duration

import cats.data.{Validated, ValidatedNel}
import com.typesafe.config.{Config, ConfigList}

trait ConfigurationHelper {

  type ErrorOr[A] = ValidatedNel[Throwable, A]

  protected implicit class AdvancedConfiguration(private val config: Config){

    def validatedString(path: String): ErrorOr[String] = wrap(_.getString(path))

    def validatedInt(path: String): ErrorOr[Int] = wrap(_.getInt(path))

    def validatedLong(path: String): ErrorOr[Long] = wrap(_.getLong(path))

    def validatedDouble(path: String): ErrorOr[Double] = wrap(_.getDouble(path))

    def validatedList(path: String): ErrorOr[ConfigList] = wrap(_.getList(path))

    def validatedConfig(path: String): ErrorOr[Config] = wrap(_.getConfig(path))

    def validatedDuration(path: String): ErrorOr[Duration] = wrap(_.getDuration(path))

    private def wrap[A](f: Config => A): ErrorOr[A] = Validated.catchNonFatal(f(config)).toValidatedNel
  }
}
