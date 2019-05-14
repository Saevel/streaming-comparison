package prv.saevel.streaming.comparison.akka.streams

import akka.Done
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._
import prv.saevel.streaming.comparison.common.model.{OriginalUser, User}
import prv.saevel.streaming.comparison.common.utils.{JsonFormats, StreamProcessor}
import spray.json._

import scala.concurrent.Future

object AkkaStreamsUserTransformation extends StreamProcessor[AkkaStreamsConfiguration, AkkaStreamsContext] with JsonFormats {

  private var optionalControl: Option[Consumer.Control] = None

  implicit val stringSerializer: Serializer[String] = new StringSerializer

  override def startStream(config: AkkaStreamsConfiguration)(implicit context: AkkaStreamsContext): Unit = {
    implicit val actorSystem = context.actorSystem
    implicit val materializer = context.materializer

    optionalControl = Option(
      originalUserSource(config)
        .map(originalUser => User(originalUser.id, originalUser.address, originalUser.country))
        .toMat(userSink(config))(Keep.left)
        .run
    )
  }

  override def stopStream(implicit context: AkkaStreamsContext): Unit = optionalControl.foreach(_.stop)

  private def userSink(config: AkkaStreamsConfiguration)(implicit context: AkkaStreamsContext): Sink[User, Future[Done]] =
    Flow[User].map(user => new ProducerRecord[String, String](config.kafka.usersOutputTopic, user.id.toString, user.toJson.toString)).toMat(
      sink(producerSettings(config, new StringSerializer))
  )((_, left) => left)

  private def sink(settings: ProducerSettings[String, String]): Sink[ProducerRecord[String, String], Future[Done]] =
    Producer.plainSink(settings)

  private def producerSettings(config: AkkaStreamsConfiguration, serializer: Serializer[String]) =
    ProducerSettings(config.other.getConfig("akka.kafka.producer"), serializer, serializer)
      .withBootstrapServers(config.kafka.bootstrapServers)

  private def originalUserSource(config: AkkaStreamsConfiguration): Source[OriginalUser, Consumer.Control] =
    Consumer.plainSource(
      ConsumerSettings(config.other.getConfig("akka.kafka.consumer"), new LongDeserializer, new StringDeserializer)
        .withBootstrapServers(config.kafka.bootstrapServers)
        .withGroupId("userTransformation")
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
      Subscriptions.topics(config.kafka.originalUsersTopic)
    ).map(record => JsonParser(record.value)).map(_.convertTo[OriginalUser])
}
