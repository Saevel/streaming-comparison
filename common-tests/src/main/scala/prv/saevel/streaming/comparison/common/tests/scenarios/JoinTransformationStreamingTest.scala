package prv.saevel.streaming.comparison.common.tests.scenarios

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer, StringDeserializer, StringSerializer}
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import prv.saevel.streaming.comparison.common.config.BasicConfig
import prv.saevel.streaming.comparison.common.model._
import prv.saevel.streaming.comparison.common.tests.utils.{NetworkUtils, StaticPropertyChecks, StreamingTest}
import prv.saevel.streaming.comparison.common.utils.{ContextProvider, JsonFormats, StreamProcessor}
import spray.json.{JsonReader, JsonWriter}

import scala.concurrent.ExecutionContext.Implicits._

abstract class JoinTransformationStreamingTest[Config <: BasicConfig, ContextType, Component <: StreamProcessor[Config, ContextType]]
  extends WordSpec with Matchers with ScalaFutures with StreamingTest[JsonWriter, JsonReader, String, String]
  with StaticPropertyChecks with ContextProvider[Config, ContextType] with IntegrationPatience with BasicGenerators
  with JsonFormats with EmbeddedKafka with NetworkUtils {

  protected val kafkaPort: Int = randomAvailablePort

  protected val zookeeperPort: Int = randomAvailablePort

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = kafkaPort, zooKeeperPort = zookeeperPort)

  type Scenario = (OriginalUser, Seq[(Account, Seq[Transaction])], Seq[AccountBalanceReport])

  protected val correctBalanceScenarios: Gen[Seq[Scenario]] = scenarios(true)

  protected val incorrectBalanceScenarios: Gen[Seq[Scenario]] = scenarios(false)

  protected implicit val stringSerializer = new StringSerializer

  protected implicit val stringDeserializer = new StringDeserializer

  protected implicit val longSerializer = new LongSerializer

  protected implicit val longDeserializer = new LongDeserializer

  protected def testBalances(config: Config, processor: Component): Unit = forOneOf(correctBalanceScenarios, incorrectBalanceScenarios) { (correctScenarios, incorrectScenarios) =>
    withRunningKafka {
      val originalUsers = (correctScenarios ++ incorrectScenarios).map{ case (originalUser, _, _) => (originalUser.id, originalUser)}

      val accounts = (correctScenarios ++ incorrectScenarios).flatMap{case (_, accountsAndTransactions, _) =>
        accountsAndTransactions.map{case (account, _) => (account.id, account)}
      }

      val transactions = (correctScenarios ++ incorrectScenarios).flatMap{case (_, accountsAndTransactions, _) =>
        accountsAndTransactions.flatMap{case (_, transactions) => transactions.map(transaction => (transaction.transactionId, transaction))}
      }

      val correctReports = correctScenarios.map{ case (_, _, report) => report}
      val incorrectReports = incorrectScenarios.map{ case (_, _, report) => report}

      val expectedReports = correctReports ++ incorrectReports

      withContext(config){ implicit context =>

        processor.startStream(config)

        val futureResults = for {
          _ <- writeStream(longSerializer, stringSerializer)(config.kafka.bootstrapServers)(config.kafka.originalUsersTopic, originalUsers)
          _ <- writeStream(longSerializer, stringSerializer)(config.kafka.bootstrapServers)(config.kafka.accountsTopic, accounts)
          _ <- writeStream(longSerializer, stringSerializer)(config.kafka.bootstrapServers)(config.kafka.transactionsTopic, transactions)
          results <- readStream[String, AccountBalanceReport, StringDeserializer, StringDeserializer](stringDeserializer, stringDeserializer)(config.kafka.bootstrapServers, config.kafka.balanceReportsTopic)
        } yield results

        whenReady(futureResults){ reports =>
          processor.stopStream
          val actualReports = reports.map(_._2)
          actualReports should contain theSameElementsAs(expectedReports)
        }
      }
    }
  }

  protected def transactionsBalance(transactions: Seq[Transaction]): Double = transactions.map{
    case Transaction(_, _, amount, TransactionType.Insertion) => amount
    case Transaction(_, _, amount, TransactionType.Withdrawal) => (-1.0) * amount
  }.fold(0.0)(_+_)

  protected def transactionsList(accountId: Long): Gen[List[Transaction]] = smallInts.flatMap(n => Gen.listOfN(n, for {
    id <- arbitrary[Long]
    transactionType <- Gen.oneOf(Seq(TransactionType.Insertion, TransactionType.Withdrawal))
    amount <- Gen.choose(10, 2000.0)
  } yield Transaction(id, accountId, amount, transactionType)))

  protected def accountsWithTransactionsAndReports(userId: Long, correct: Boolean): Gen[List[(Account, Seq[Transaction], AccountBalanceReport)]] =
    smallInts.flatMap(n => Gen.listOfN(n, for {
      accountId <- arbitrary[Long]
      transactions <- transactionsList(accountId)
      transBalance <- Gen.const(transactionsBalance(transactions))
      accountBalance <- if(correct) Gen.const(transactionsBalance(transactions)) else Gen.choose(-5000.0, 5000.0)
    } yield (
      Account(accountId, userId, accountBalance),
      transactions,
      AccountBalanceReport(userId, accountId, accountBalance, transBalance, correct)
    )))

  protected def scenarios(correct: Boolean): Gen[Seq[Scenario]] = smallInts.flatMap(Gen.listOfN(_, for {
    userId <- arbitrary[Long]
    name <- names
    surname <- surnames
    age <- Gen.choose(18, 100)
    country <- countries
    address <- addresses(country)
    accountsWithTransactionsWithReports <- accountsWithTransactionsAndReports(userId, correct)
  } yield (OriginalUser(userId, name, surname, address, age, country),
    accountsWithTransactionsWithReports.map{ case (account, transactions, _) => (account, transactions)},
    accountsWithTransactionsWithReports.map{ case (_, _, report) => report}
  )))
}
