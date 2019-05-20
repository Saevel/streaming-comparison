package prv.saevel.streaming.comparison.flink

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.streaming.comparison.common.config.KafkaConfiguration
import prv.saevel.streaming.comparison.common.tests.scenarios.JoinTransformationStreamingTest
import prv.saevel.streaming.comparison.common.tests.utils.{JsonStreamReader, JsonStreamWriter}
import prv.saevel.streaming.comparison.common.utils.JsonFormats

@RunWith(classOf[JUnitRunner])
class FlinkAccountBalanceVerificationTest extends JoinTransformationStreamingTest[FlinkConfiguration, StreamExecutionEnvironment, FlinkAccountBalanceVerification.type]
  with FlinkContextProvider with JsonFormats with JsonStreamReader with JsonStreamWriter {

  import scala.concurrent.duration._

  private val configuration = new FlinkConfiguration(
    KafkaConfiguration(s"127.0.0.1:${kafkaPort}", "original_users", "users", "accounts", "transactions","balance_reports", "statistics"),
    30 seconds,
    "FlinkStreamingUserTransformationTest"
  )

  "FlinkAccountBalanceVerficiation" when {

    "given scenarios for correct balance verification" should {

      "stream them to the appropriate output topic" in testCorrectBalances(configuration, FlinkAccountBalanceVerification)
    }

    "given scenarios for incorrect balance verification" should {

      "stream them to the appropriate output topic" in testIncorrectBalances(configuration, FlinkAccountBalanceVerification)
    }
  }
}
