package prv.saevel.streaming.comparison.common.tests.utils

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Serializer
import spray.json.JsonWriter

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait JsonStreamWriter extends StreamWriter[JsonWriter, String]{

  import spray.json._

  override def writeStream[KeyType, DataType, KS <: Serializer[_], DS <: Serializer[_]](keySerializer: KS,
                                                                                        valueSerializer: DS)
                                                                                       (bootstrapServers: String)
                                                                                       (topic: String, data: Seq[(KeyType, DataType)])
                                                                                       (implicit format: JsonWriter[DataType],
                                                                                        ex: ExecutionContext): Future[_] =
    withKafkaProducer[Future[_], KeyType, String, KS, DS](keySerializer, valueSerializer)(bootstrapServers)( producer =>
      data
        .map{ case (key, value) => new ProducerRecord(topic, key, value.toJson.toString)}
        .map(producer.send)
        .map(future => Future(future.get(500, TimeUnit.MILLISECONDS)))
        .fold(Future.successful({}))((f1, f2) => f1.flatMap(_ => f2))
    )

  protected def withKafkaProducer[T, KeyType, DataType, KS <: Serializer[_], DS <: Serializer[_]](keySerializer: KS,
                                                                                                  valueSerializer: DS)(bootstrapServers: String)
                                                                                                 (f: KafkaProducer[KeyType, DataType] => T): T = {
    val producerAttempt = Try(new KafkaProducer[KeyType, DataType](Map[String, AnyRef](
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> keySerializer.getClass.getName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> valueSerializer.getClass.getName,
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers
    ).asJava))

    val resultAttempt = producerAttempt.map(f)

    producerAttempt.foreach(_.close)

    resultAttempt match {
      case Success(t) => t
      case Failure(e) => throw e
    }
  }
}
