package prv.saevel.streaming.comparison.common.tests.scenarios

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer, StringDeserializer, StringSerializer}
import org.scalacheck.Gen
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{Matchers, WordSpec}
import prv.saevel.streaming.comparison.common.model._
import prv.saevel.streaming.comparison.common.tests.utils.{NetworkUtils, StaticPropertyChecks, StreamingTest}
import org.scalacheck.Arbitrary._
import org.scalatest.time.{Minutes, Span}
import prv.saevel.streaming.comparison.common.config.BasicConfig
import prv.saevel.streaming.comparison.common.utils.{ContextProvider, JsonFormats, StreamProcessor}
import spray.json.{JsonReader, JsonWriter}

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

abstract class UserTransformationStreamingTest[Config <: BasicConfig, ContextType, Component <: StreamProcessor[Config, ContextType]]
  extends WordSpec with Matchers with ScalaFutures with StreamingTest[JsonWriter, JsonReader, String, String]
    with StaticPropertyChecks with ContextProvider[Config, ContextType] with IntegrationPatience with BasicGenerators
    with EmbeddedKafka with NetworkUtils with JsonFormats {

  override implicit val patienceConfig = PatienceConfig.apply(timeout = Span(3, Minutes))

  protected val kafkaPort: Int = randomAvailablePort

  protected val zookeeperPort: Int = randomAvailablePort

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = kafkaPort,
    zooKeeperPort = zookeeperPort,
    customBrokerProperties = Map("zookeeper.connection.timeout.ms" -> "5000")
  )

  implicit val stringSerializer = new StringSerializer

  implicit val stringDeserializer = new StringDeserializer

  implicit val longSerializer = new LongSerializer

  implicit val longDeserializer = new LongDeserializer

  protected val originalAndTransformedUsers: Gen[Seq[(OriginalUser,User)]] = mediumInts.flatMap(n => Gen.listOfN(n, for {
    id <- arbitrary[Long]
    name <- names
    surname <- surnames
    age <- Gen.choose(18, 99)
    country <- countries
    address <- addresses(country)
  } yield  (OriginalUser(id, name, surname, address, age, country), User(id, address, country))))

  protected def testUserTransformation(config: Config, processor: Component): Unit = forOneOf(originalAndTransformedUsers) { data =>
    withRunningKafka {
      val originalUsers = data.map{case (originalUser, _) => (originalUser.id, originalUser)}
      val transformedUsers = data.map{case (_, user) => user}

      withContext(config){ implicit context =>

        processor.startStream(config)

        val futureOutput = for {
          _ <- writeStream(longSerializer, stringSerializer)(config.kafka.bootstrapServers)(config.kafka.originalUsersTopic, originalUsers)
          _ <- Future(Thread.sleep(90 * 1000))
          out <- readStream[String, User, StringDeserializer, StringDeserializer](stringDeserializer, stringDeserializer)(config.kafka.bootstrapServers, config.kafka.usersOutputTopic)
        } yield out

        whenReady(futureOutput){ users =>
          processor.stopStream
          val receivedUsers = users.map{ case (_, user) => user}
          receivedUsers should contain theSameElementsAs(transformedUsers)
        }
      }
    }
  }
}
