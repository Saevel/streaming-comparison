package prv.saevel.streaming.comparison.common.utils

import java.io.File

import cats.data.{Validated, ValidatedNel}
import com.typesafe.config.{Config, ConfigFactory}

trait ExternalConfiguration {

  protected val configFilePath: ValidatedNel[Throwable, String] = Validated.catchNonFatal(System.getProperty("CONFIG_FILE")).toValidatedNel

  protected val rawConfig: ValidatedNel[Throwable, Config] = configFilePath.map(path => ConfigFactory.parseFile(new File(path)))
}
