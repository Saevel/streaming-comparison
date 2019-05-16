package prv.saevel.streaming.comparison.akka.streams

import akka.Done
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._

import scala.concurrent.Future

trait KafkaIO {

  protected def consumerSettings[K, V, KD <: Deserializer[K], VD <: Deserializer[V]](bootstrapServers: String,
                                                                                     groupId: String,
                                                                                     otherSettings: Config,
                                                                                     kd: KD,
                                                                                     vd: VD): ConsumerSettings[K, V] =
    ConsumerSettings(otherSettings, kd, vd)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


  protected def source[K, V](settings: ConsumerSettings[K, V], topic: String): Source[(K, V), Consumer.Control] =
    Consumer.plainSource(settings, Subscriptions.topics(topic)).map(record => (record.key, record.value))

  protected def producerSettings[K, V, KS <: Serializer[K], VS <: Serializer[V]](bootstrapServers: String,
                                                                               otherConfig: Config,
                                                                               ks: KS,
                                                                               vs: VS): ProducerSettings[K, V] =
    ProducerSettings(otherConfig, ks, vs).withBootstrapServers(bootstrapServers)

  protected def sink[K, V](settings: ProducerSettings[K, V], topic: String): Sink[(K, V), Future[Done]] =
    Flow[(K, V)].map{case (key, value) => new ProducerRecord[K, V](topic, key, value)}.toMat(
      Producer.plainSink(settings)
    )((_, done) => done)
}
