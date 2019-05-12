package prv.saevel.streaming.comparison.spark.sturctured.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import prv.saevel.streaming.comparison.common.model.{OriginalUser, User}
import prv.saevel.streaming.comparison.common.utils.StreamProcessor

object SparkStreamingUserTransformation extends StreamProcessor[SparkConfiguration, SparkSession]{

  private val queryName = "UserTransformationQuery"

  private val originalUserSchema = StructType(Seq(
    StructField("id", LongType),
    StructField("name", StringType),
    StructField("surname", StringType),
    StructField("address", StringType),
    StructField("age", IntegerType),
    StructField("country", StringType)
  ))

  override def startStream(config: SparkConfiguration)(implicit context: SparkSession): Unit = {

    import context.implicits._

    context
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("subscribe", config.kafka.originalUsersTopic)
      .option("startingOffsets", "earliest")
      .load
      .select(from_json($"value".cast("string"), originalUserSchema).as[OriginalUser])
      .map(originalUser => (User(originalUser.id, originalUser.address, originalUser.country)))
      .select($"id".as("key"), to_json(struct("*")).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafka.bootstrapServers)
      .option("topic", config.kafka.usersOutputTopic)
      .queryName(queryName)
      .outputMode(OutputMode.Append)
      .start
  }

  override def stopStream(implicit context: SparkSession): Unit = context.streams.get(queryName).stop
}
