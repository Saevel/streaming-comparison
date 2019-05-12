package prv.saevel.streaming.comparison.common.tests.scenarios

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer, LongSerializer, StringSerializer}
import org.scalacheck.Gen
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.{Matchers, WordSpec}
import prv.saevel.streaming.comparison.common.model._
import prv.saevel.streaming.comparison.common.tests.utils.{NetworkUtils, StaticPropertyChecks, StreamingTest}
import org.scalacheck.Arbitrary._
import prv.saevel.streaming.comparison.common.config.BasicConfig
import prv.saevel.streaming.comparison.common.utils.{ContextProvider, JsonFormats, StreamProcessor}
import spray.json.{JsonReader, JsonWriter}

import scala.concurrent.ExecutionContext.Implicits._

abstract class UserTransformationStreamingTest[Config <: BasicConfig, ContextType, Component <: StreamProcessor[Config, ContextType]]
  extends WordSpec with Matchers with ScalaFutures with StreamingTest[JsonWriter, JsonReader, String, String]
    with StaticPropertyChecks with ContextProvider[Config, ContextType] with IntegrationPatience with BasicGenerators
    with EmbeddedKafka with NetworkUtils with JsonFormats {

  protected val kafkaPort: Int = randomAvailablePort

  protected val zookeeperPort: Int = randomAvailablePort

  implicit val embeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = kafkaPort, zooKeeperPort = zookeeperPort)

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
      val transformedUsers = data.map{case (_, user) => (user.id, user)}

      withContext(config){ implicit context =>

        processor.startStream(config)

        val futureOutput = for {
          _ <- writeStream(longSerializer, stringSerializer)(config.kafka.bootstrapServers)(config.kafka.originalUsersTopic, originalUsers)
          out <- readStream[Long, User, LongDeserializer, StringDeserializer](longDeserializer, stringDeserializer)(config.kafka.bootstrapServers, config.kafka.usersOutputTopic)
        } yield out

        whenReady(futureOutput){ users =>
          processor.stopStream
          users should contain theSameElementsAs(transformedUsers)
        }
      }
    }
  }
}
