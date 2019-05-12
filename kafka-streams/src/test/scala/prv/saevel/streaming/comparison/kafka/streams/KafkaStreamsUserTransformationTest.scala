package prv.saevel.streaming.comparison.kafka.streams

import org.apache.kafka.streams.scala.StreamsBuilder
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import prv.saevel.streaming.comparison.common.tests.scenarios.UserTransformationStreamingTest

@RunWith(classOf[JUnitRunner])
class KafkaStreamsUserTransformationTest extends UserTransformationStreamingTest[KafkaStreamsConfiguration, StreamsBuilder, KafkaStreamsUserTransformation.type]
  with KafkaStreamsContextProvider {

  override protected val config: KafkaStreamsConfiguration = ???

  override protected val processor: KafkaStreamsUserTransformation.type = KafkaStreamsUserTransformation
}
