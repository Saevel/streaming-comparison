package prv.saevel.streaming.comparison.kafka.streams

import java.time.Duration

import org.apache.kafka.streams.scala.StreamsBuilder
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.streaming.comparison.common.config.KafkaConfiguration
import prv.saevel.streaming.comparison.common.tests.scenarios.JoinTransformationStreamingTest
import prv.saevel.streaming.comparison.common.tests.utils.{JsonStreamReader, JsonStreamWriter}

@RunWith(classOf[JUnitRunner])
class KafkaStreamsAccountBalanceVerificationTest extends JoinTransformationStreamingTest[KafkaStreamsConfiguration, StreamsBuilder, KafkaStreamsBalanceVerification.type]
  with KafkaStreamsContextProvider with JsonStreamWriter with JsonStreamReader {

  private val configuration: KafkaStreamsConfiguration = KafkaStreamsConfiguration(
    KafkaConfiguration(s"127.0.0.1:${kafkaPort}", "original_users", "users", "accounts", "transactions","balance_reports", "statistics"),
    Duration.ofSeconds(30),
    "KafkaStreamsAccountBalanceVerificationTest",
  )

  "KafkaStreamsAccountBalanceVerification" when {

    "given scenarios for correct balance verification" should {

      "stream them to the appropriate output topic" in testCorrectBalances(configuration, KafkaStreamsBalanceVerification)
    }

    "given scenarios for incorrect balance verification" should {

      "stream them to the appropriate output topic" in testIncorrectBalances(configuration, KafkaStreamsBalanceVerification)
    }
  }
}
