package prv.saevel.streaming.comparison.spark.sturctured.streaming

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}
import prv.saevel.streaming.comparison.spark.sturctured.streaming.SparkStreamingUserTransformation.queryName

trait SparkKafkaIO {

  protected def fromKafka(topic: String, config: SparkConfiguration)
                         (implicit session: SparkSession): Dataset[_] =
    session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load


  implicit protected class KafkaSink(stream: Dataset[_]){

    def toKafka(topic: String, queryName: String, config: SparkConfiguration)(implicit session: SparkSession): Unit =
      stream
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
        .option("topic", config.kafka.usersOutputTopic)
        .queryName(queryName)
        .outputMode(OutputMode.Append)
        .start
  }
}
