package prv.saevel.streaming.comparison.spark.sturctured.streaming
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}

trait KafkaIO {

  protected def fromKafka(config: SparkConfiguration, topic: String)(implicit session: SparkSession): Dataset[_] =
    session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load

  protected implicit class ToKafka(data: Dataset[_]){

    def toKafkaTopic(bootstrapServers: String, topic: String, queryName: String): Unit =
      data
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("topic", topic)
        .queryName(queryName)
        .outputMode(OutputMode.Append)
        .start
  }
}
