package prv.saevel.streaming.comparison.spark.structured.streaming

import java.time.Duration

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.streaming.comparison.common.config.KafkaConfiguration
import prv.saevel.streaming.comparison.common.tests.scenarios.JoinTransformationStreamingTest
import prv.saevel.streaming.comparison.common.tests.utils.{JsonStreamReader, JsonStreamWriter}
import prv.saevel.streaming.comparison.spark.sturctured.streaming.{SparkConfiguration, SparkStreamingBalanceVerification}

@RunWith(classOf[JUnitRunner])
class SparkStreamingAccountBalanceVerificationTest extends JoinTransformationStreamingTest[SparkConfiguration, SparkSession, SparkStreamingBalanceVerification.type]
  with SparkSessionProvider with JsonStreamReader with JsonStreamWriter {

  private val config: SparkConfiguration = SparkConfiguration(
    KafkaConfiguration(s"127.0.0.1:${kafkaPort}", "original_users", "users", "accounts", "transactions", "balance_reports", "statistics"),
    Duration.ofSeconds(30),
    "SparkStreamingAccountBalanceVerificationTest",
    "file:///checkpoints",
    Duration.ofSeconds(10)
  )

  "SparkStreamingAccountBalanceVerification" when {

    "given Accounts and matching Transactions on appropriate Kafka topics" should {

      "join them and verify account balance" in testBalances(config, SparkStreamingBalanceVerification)
    }
  }
}
