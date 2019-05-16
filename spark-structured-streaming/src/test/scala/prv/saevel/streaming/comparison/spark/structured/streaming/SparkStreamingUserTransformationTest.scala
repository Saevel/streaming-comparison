package prv.saevel.streaming.comparison.spark.structured.streaming

import java.time.Duration

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.streaming.comparison.common.config.KafkaConfiguration
import prv.saevel.streaming.comparison.common.tests.scenarios.UserTransformationStreamingTest
import prv.saevel.streaming.comparison.common.tests.utils.{JsonStreamReader, JsonStreamWriter}
import prv.saevel.streaming.comparison.spark.sturctured.streaming.{SparkConfiguration, SparkStreamingUserTransformation}


@RunWith(classOf[JUnitRunner])
class SparkStreamingUserTransformationTest extends UserTransformationStreamingTest[SparkConfiguration, SparkSession, SparkStreamingUserTransformation.type]
  with SparkSessionProvider with JsonStreamReader with JsonStreamWriter {

  private val config: SparkConfiguration = SparkConfiguration(
    KafkaConfiguration(s"127.0.0.1:${kafkaPort}", "original_users", "users", "accounts", "transactions","balance_reports", "statistics"),
    Duration.ofSeconds(30),
    "SparkStreamingUserTransformationTest",
    "file:///checkpoints",
    Duration.ofSeconds(3)
  )

  private val processor: SparkStreamingUserTransformation.type = SparkStreamingUserTransformation

  "SparkStreamingUserTransformation" when {

    "provided a stream of OriginalUsers" should {

      "transform them into Users" in testUserTransformation(config, processor)
    }
  }
}