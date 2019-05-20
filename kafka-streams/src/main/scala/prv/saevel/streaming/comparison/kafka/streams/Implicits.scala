package prv.saevel.streaming.comparison.kafka.streams
import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.kstream.{Joined, Produced, Serialized}

object Implicits {

  /**
  implicit def implicitSerialized[K, V](implicit keySerializer: Serde[K], valueSerializer: Serde[V]): Serialized[K, V] =
    Serialized.`with`(keySerializer, valueSerializer)
    */

  /**
  implicit def implicitProduced[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): Produced[K, V] =
    Produced.`with`(keySerde, valueSerde)
    */

/**
  implicit def implicitJoined[K, V1, V2](implicit keySerde: Serde[K], value1Serde: Serde[V1], value2Serde: Serde[V2]): Joined[K, V1, V2] =
    Joined.`with`(keySerde, value1Serde, value2Serde)
*/

  implicit def implicitSerde[T](implicit s: Serializer[T], d: Deserializer[T]): Serde[T] = new Serde[T] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
      s.configure(configs, isKey)
      d.configure(configs, isKey)
    }

    override def close(): Unit = {
      s.close()
      d.close()
    }

    override def serializer(): Serializer[T] = s

    override def deserializer(): Deserializer[T] = d
  }
}
