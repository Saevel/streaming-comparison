package prv.saevel.streaming.comparison.common.tests.scenarios

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{LongSerializer, StringDeserializer, StringSerializer, LongDeserializer}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import prv.saevel.streaming.comparison.common.config.BasicConfig
import prv.saevel.streaming.comparison.common.model._
import prv.saevel.streaming.comparison.common.tests.utils.{NetworkUtils, StaticPropertyChecks, StreamingTest}
import prv.saevel.streaming.comparison.common.utils.{ContextProvider, JsonFormats, StreamProcessor}
import spray.json.{JsonReader, JsonWriter}

import scala.concurrent.ExecutionContext.Implicits._

abstract class AggregationStreamingTest[Config <: BasicConfig, ContextType, Component <: StreamProcessor[Config, ContextType]]
  extends WordSpec with Matchers with ScalaFutures with StaticPropertyChecks with IntegrationPatience with BasicGenerators
  with StreamingTest[JsonWriter, JsonReader, String, String] with ContextProvider[Config, ContextType] with JsonFormats
  with NetworkUtils with EmbeddedKafka {

  import spray.json._

  protected val kafkaPort: Int = randomAvailablePort

  protected val zookeeperPort: Int = randomAvailablePort

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = kafkaPort, zooKeeperPort = zookeeperPort)

  protected val processor: Component

  protected val config: Config

  protected val componentName: String

  type DataEnsemble = Seq[(OriginalUser, Seq[(Account, Seq[Transaction])])]

  case class Scenario(data: DataEnsemble, statistics: Seq[Statistic])

  protected val scenarios: Gen[Scenario] = for {
    data <- usersWithAccountsAndTransactions
  } yield Scenario(data, calculateStats(data))

  protected def transactions(accountId: Long): Gen[Seq[Transaction]] = smallInts.flatMap(Gen.listOfN(_, for {
    amount <- Gen.choose(10.0, 2000.0)
    id <- arbitrary[Long]
    transactionType <- Gen.oneOf(Seq(Insertion, Withdrawal))
  } yield Transaction(id, accountId, amount, transactionType)))

  protected def accountsWithTransactions(userId: Long): Gen[Seq[(Account, Seq[Transaction])]] = smallInts.flatMap(Gen.listOfN(_, for {
    accountId <- arbitrary[Long]
    balance <- Gen.choose(-5000.0, 5000.0)
    trans <- transactions(accountId)
  } yield (Account(accountId, userId, balance), trans)))

  protected def usersWithAccountsAndTransactions: Gen[DataEnsemble] = smallInts.flatMap(Gen.listOfN(_, for {
    userId <- arbitrary[Long]
    accountsAndTransactions <- accountsWithTransactions(userId)
    userId <- arbitrary[Long]
    name <- names
    surname <- surnames
    age <- Gen.choose(18, 100)
    country <- countries
    address <- addresses(country)
  } yield (OriginalUser(userId, name, surname, address, age, country), accountsAndTransactions)))

  implicit val stringSerializer = new StringSerializer

  implicit val stringDeserialier = new StringDeserializer

  implicit val longSerializer = new LongSerializer

  implicit val longDeserializer = new LongDeserializer

  componentName when {

    "given a stream of Users, Accounts and Transactions" should {

      "join them on ids and calculate the average Insertion, Withdrawal and Transfer amount by country" in forOneOf(scenarios) { scenario =>

        withRunningKafka {

          val users = scenario.data.map{ case (user, _) => (user.id, user)}
          val accounts = scenario.data.flatMap{ case (_, accountsAndTransactions) =>
            accountsAndTransactions.map{ case (account, _) => (account.id, account)}
          }
          val transactions = scenario.data.flatMap{ case (_, accountsAndTransactions) =>
            accountsAndTransactions.flatMap{ case (_, transactions) => transactions.map(t => (t.id, t))}
          }

          withContext(config){ implicit context =>

            processor.startStream(config)

            val futureResults = for {
              _ <- writeStream(longSerializer, stringSerializer)(config.kafka.bootstrapServers)(config.kafka.originalUsersTopic, users)
              _ <- writeStream(longSerializer, stringSerializer)(config.kafka.bootstrapServers)(config.kafka.accountsTopic, accounts)
              _ <- writeStream(longSerializer, stringSerializer)(config.kafka.bootstrapServers)(config.kafka.transactionsTopic, transactions)
              results <- readStream[Long, Statistic, LongDeserializer, StringDeserializer](longDeserializer, stringDeserialier)(config.kafka.bootstrapServers,
                config.kafka.statisticsTopic)
            } yield results

            whenReady(futureResults){ statistics =>
              processor.stopStream
              statistics should contain theSameElementsAs(scenario.statistics)
            }
          }
        }
      }
    }
  }

  protected def calculateStats(data: DataEnsemble): Seq[Statistic] =
    data
      .flat
      .groupBy{ case (user, _, transaction) => (user.country, transaction.transactionType)}
      .map {case ((country, transactionType), records) => Statistic(
        country,
        transactionType,
        if(records.isEmpty) 0.0 else records.map{ case (_, _, transaction) => transaction.value}.fold(0.0)(_+_) / records.size
      )}
      .toSeq

  protected implicit class FlattenedJoin3[A, B, C](data: Seq[(A, Seq[(B, Seq[C])])]){

    def flat: Seq[(A, B, C)] = for {
      (a, bs) <- data
      (b, cs) <- bs
      c <- cs
    } yield (a, b, c)
  }
}
