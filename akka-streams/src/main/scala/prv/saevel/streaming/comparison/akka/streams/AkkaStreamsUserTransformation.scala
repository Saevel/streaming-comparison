package prv.saevel.streaming.comparison.akka.streams

import akka.Done
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{LongDeserializer, LongSerializer, StringDeserializer, StringSerializer}
import prv.saevel.streaming.comparison.common.model.{OriginalUser, User}
import prv.saevel.streaming.comparison.common.utils.{JsonFormats, StreamProcessor}
import spray.json._

import scala.concurrent.Future

object AkkaStreamsUserTransformation extends StreamProcessor[AkkaStreamsConfiguration, AkkaStreamsContext] with JsonFormats {

  private var optionalControl: Option[Consumer.Control] = None

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

  // FIXME: Ambiguous method call?
  private def userSink(config: AkkaStreamsConfiguration)(implicit context: AkkaStreamsContext): Sink[User, Future[Done]] =
    Flow[User].map(user => new ProducerRecord(config.kafka.usersOutputTopic, user.id, user.toJson.toString)).toMat(
      Producer.plainSink(ProducerSettings.create(config.other, new LongSerializer, new StringSerializer))
    )(Keep.left)

  private def originalUserSource(config: AkkaStreamsConfiguration): Source[OriginalUser, Consumer.Control] =
    Consumer.plainSource(
      ConsumerSettings(config.other, new LongDeserializer, new StringDeserializer),
      Subscriptions.topics(config.kafka.originalUsersTopic)
    ).map(record => JsonParser(record.value)).map(_.convertTo[OriginalUser])
}
