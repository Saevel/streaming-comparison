package prv.saevel.streaming.comparison.common.tests.utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.Deserializer
import spray.json.JsonReader

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

trait JsonStreamReader extends StreamReader[JsonReader, String] {

  override def readStream[KeyType, DataType, KD <: Deserializer[_], VD <: Deserializer[_]](keyDeserializer: KD,
    valueDeserializer: VD)(bootstrapServers: String, topic: String, timeout: Duration = 5 seconds, maxIterations: Long = 100)
    (implicit deserializer: JsonReader[DataType], ex: ExecutionContext): Future[Seq[(KeyType, DataType)]] =
    withKafkaConsumer[Future[Seq[(KeyType, DataType)]], KeyType, DataType, KD, VD](keyDeserializer, valueDeserializer)(bootstrapServers, topic){ consumer =>
      Future {
        var results = Seq.empty[(KeyType, DataType)]
        var i = 0;
        val startTime = System.currentTimeMillis
        var duration: Long = 0
        while(i > maxIterations || duration >= timeout.toMillis){
          results = results ++ consumer.poll(100).iterator.asScala.toSeq.map(record => (record.key, record.value))
          duration = System.currentTimeMillis - startTime
          i = i + 1
        }

        results
      }
    }

  protected def withKafkaConsumer[T, KeyType, DataType, KD <: Deserializer[_], VD <: Deserializer[_]](keyDeserializer: KD,
                                                                                                                 valueDeserializer: VD)
                                                                                                                (bootstrapServers: String,
                                                                                                                 topic: String)(f: KafkaConsumer[KeyType, DataType] => T): T = {
    val consumerTrial = Try {

      val consumer = new KafkaConsumer[KeyType, DataType](Map[String, AnyRef](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> keyDeserializer.getClass.getName,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> valueDeserializer.getClass.getName,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
      ).asJava)

      consumer.subscribe(Set(topic).asJava)

      consumer
    }

    val resultTrial = consumerTrial.map(f)

    consumerTrial.foreach(_.close)

    resultTrial match {
      case Success(t) => t
      case Failure(e) => throw e
    }
  }
}
