package prv.saevel.streaming.comparison.flink

import java.time.Duration

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.streaming.comparison.common.config.KafkaConfiguration
import prv.saevel.streaming.comparison.common.tests.scenarios.UserTransformationStreamingTest
import prv.saevel.streaming.comparison.common.tests.utils.{JsonStreamReader, JsonStreamWriter}

@RunWith(classOf[JUnitRunner])
class FlinkUserTransformationTest extends UserTransformationStreamingTest[FlinkConfiguration, StreamExecutionEnvironment, FlinkUserTransformation.type]
  with JsonStreamWriter with JsonStreamReader with FlinkContextProvider {

  private val config: FlinkConfiguration = new FlinkConfiguration(
    KafkaConfiguration(s"127.0.0.1:$kafkaPort", "original_users", "users", "accounts", "transactions","balance_reports", "statistics"),
    Duration.ofSeconds(30),
    "FlinkUserTransformationTest"
  )

  "FlinkUserTransformation" when {

    "given a number of OriginalUsers on a Kafka topic" should {

      "transform them to Users" in testUserTransformation(config, FlinkUserTransformation)
    }
  }
}
