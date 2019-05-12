package prv.saevel.streaming.comparison.spark.structured.streaming

import org.apache.spark.sql.SparkSession
import prv.saevel.streaming.comparison.common.utils.ContextProvider
import prv.saevel.streaming.comparison.spark.sturctured.streaming.SparkConfiguration

import scala.util.{Failure, Success, Try}

trait SparkSessionProvider extends ContextProvider[SparkConfiguration, SparkSession]{

  override def withContext[T](config: SparkConfiguration)(f: SparkSession => T): T = {
    val trySession = Try(
      SparkSession
        .builder()
        .master("local[*]")
        .appName(config.applicationName)
        .config("spark.sql.streaming.checkpointLocation", config.checkpointLocation)
        .getOrCreate
    )

    val resultAttempt = trySession.map(f)

    trySession.foreach(_.stop)

    resultAttempt match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }
}
