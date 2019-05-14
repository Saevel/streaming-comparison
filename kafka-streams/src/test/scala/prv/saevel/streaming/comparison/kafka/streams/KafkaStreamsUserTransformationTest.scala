package prv.saevel.streaming.comparison.kafka.streams

import java.time.Duration

import org.apache.kafka.streams.scala.StreamsBuilder
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.streaming.comparison.common.config.KafkaConfiguration
import prv.saevel.streaming.comparison.common.tests.scenarios.UserTransformationStreamingTest
import prv.saevel.streaming.comparison.common.tests.utils.{JsonStreamReader, JsonStreamWriter}

@RunWith(classOf[JUnitRunner])
class KafkaStreamsUserTransformationTest extends UserTransformationStreamingTest[KafkaStreamsConfiguration, StreamsBuilder, KafkaStreamsUserTransformation.type]
  with KafkaStreamsContextProvider with JsonStreamWriter with JsonStreamReader {

  private val config: KafkaStreamsConfiguration = KafkaStreamsConfiguration(
    KafkaConfiguration(s"127.0.0.1:${kafkaPort}", "original_users", "users", "accounts", "transactions","balance_reports", "statistics"),
    Duration.ofSeconds(30),
    "KafkaStreamsUserTransformationTest",
  )

  private val processor: KafkaStreamsUserTransformation.type = KafkaStreamsUserTransformation

  "KafkaStreamsUserTransformationTest" when {

    "given a stream of OriginalUsers on a Kafka topic" should {

      "transform the to Users" in testUserTransformation(config, processor)
    }
  }
}
