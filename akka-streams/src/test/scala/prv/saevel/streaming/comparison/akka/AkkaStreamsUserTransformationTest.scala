package prv.saevel.streaming.comparison.akka

import java.time.Duration

import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.streaming.comparison.akka.streams.{AkkaStreamsConfiguration, AkkaStreamsContext, AkkaStreamsContextProvider, AkkaStreamsUserTransformation}
import prv.saevel.streaming.comparison.common.config.KafkaConfiguration
import prv.saevel.streaming.comparison.common.tests.scenarios.UserTransformationStreamingTest
import prv.saevel.streaming.comparison.common.tests.utils.{JsonStreamReader, JsonStreamWriter}

@RunWith(classOf[JUnitRunner])
class AkkaStreamsUserTransformationTest extends UserTransformationStreamingTest[AkkaStreamsConfiguration, AkkaStreamsContext, AkkaStreamsUserTransformation.type]
  with JsonStreamReader with JsonStreamWriter with AkkaStreamsContextProvider {

  private val config: AkkaStreamsConfiguration = AkkaStreamsConfiguration(
    KafkaConfiguration(s"127.0.0.1:$kafkaPort", "original_users", "users", "accounts", "transactions","balance_reports", "statistics"),
    Duration.ofSeconds(30),
    "KafkaStreamsUserTransformationTest",
    ConfigFactory.parseString("""
      akka.kafka.producer {
        # Tuning parameter of how many sends that can run in parallel.
        parallelism = 100

        # Duration to wait for `KafkaConsumer.close` to finish.
        close-timeout = 60s

        # Fully qualified config path which holds the dispatcher configuration
        # to be used by the producer stages. Some blocking may occur.
        # When this value is empty, the dispatcher configured for the stream
        # will be used.
        use-dispatcher = "akka.kafka.default-dispatcher"

        # The time interval to commit a transaction when using the `Transactional.sink` or `Transactional.flow`
        # for exactly-once-semantics processing.
        eos-commit-interval = 100ms

        # Properties defined by org.apache.kafka.clients.producer.ProducerConfig
        # can be defined in this configuration section.
        kafka-clients {
          enable.auto.commit = true
        }
      }

      akka.kafka.consumer {
        poll-interval = 50ms
        poll-timeout = 50ms
        stop-timeout = 30s
        close-timeout = 20s
        commit-timeout = 15s
        commit-time-warning = 1s
        wakeup-timeout = 3s
        max-wakeups = 10
        commit-refresh-interval = infinite
        wakeup-debug = true
        use-dispatcher = "akka.kafka.default-dispatcher"
        wait-close-partition = 500ms
        position-timeout = 5s
        offset-for-times-timeout = 5s
        metadata-request-timeout = 5s
        eos-draining-check-interval = 30ms

        kafka-clients {
          enable.auto.commit = true
        }
      }

      kafka-clients {
        enable.auto.commit = true
      }""")
  )

  private val processor: AkkaStreamsUserTransformation.type = AkkaStreamsUserTransformation

  "AkkaStreamsUserTransformation" when {

    "given a number of OriginalUsers on a Kafka topic" should {

      "transform them to Users" in testUserTransformation(config, processor)
    }
  }
}
