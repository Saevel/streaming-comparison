package prv.saevel.streaming.comparison.kafka.streams

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder
import prv.saevel.streaming.comparison.common.model.{OriginalUser, User}
import prv.saevel.streaming.comparison.common.utils.{JsonFormats, StreamProcessor}

object KafkaStreamsUserTransformation extends StreamProcessor[KafkaStreamsConfiguration, StreamsBuilder] with JsonFormats {

  import prv.saevel.streaming.comparison.kafka.streams.Implicits._
  import spray.json._

  private var optionalStreams: Option[KafkaStreams] = None

  override def startStream(config: KafkaStreamsConfiguration)(implicit context: StreamsBuilder): Unit = {
    import org.apache.kafka.streams.scala.Serdes._
    import org.apache.kafka.streams.scala.ImplicitConversions._

    import spray.json._

    context
      .stream[String, String](config.kafka.originalUsersTopic)
      .mapValues(s => JsonParser(s).convertTo[OriginalUser])
      .mapValues(originalUser => User(originalUser.id, originalUser.address, originalUser.country))
      .mapValues(_.toJson.toString)
      .to(config.kafka.usersOutputTopic)

    optionalStreams = Option(new KafkaStreams(context.build, config.toProperties))
    optionalStreams.foreach(_.start)
  }

  override def stopStream(implicit context: StreamsBuilder): Unit = optionalStreams.foreach(_.close)
}