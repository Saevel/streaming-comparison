package prv.saevel.streaming.comparison.kafka.streams

import org.apache.kafka.streams.scala.StreamsBuilder
import prv.saevel.streaming.comparison.common.utils.ContextProvider

trait KafkaStreamsContextProvider extends ContextProvider[KafkaStreamsConfiguration, StreamsBuilder]{
  override def withContext[T](config: KafkaStreamsConfiguration)(f: StreamsBuilder => T): T =
    f(new StreamsBuilder)
}
