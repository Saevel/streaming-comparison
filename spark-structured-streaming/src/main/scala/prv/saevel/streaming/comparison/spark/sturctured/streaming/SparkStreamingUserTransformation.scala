package prv.saevel.streaming.comparison.spark.sturctured.streaming

import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import prv.saevel.streaming.comparison.common.model.{OriginalUser, User}
import prv.saevel.streaming.comparison.common.utils.StreamProcessor

object SparkStreamingUserTransformation extends StreamProcessor[SparkConfiguration, SparkSession] with KafkaIO {

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

    fromKafka(config, config.kafka.originalUsersTopic)
      .select(from_json($"value".cast(StringType), originalUserSchema).as[OriginalUser])
      .map(originalUser => (User(originalUser.id, originalUser.address, originalUser.country)))
      .select($"id".cast(StringType).as("key"), to_json(struct("*")).as("value"))
      .toKafkaTopic(config.kafka.bootstrapServers, config.kafka.usersOutputTopic, queryName)
  }

  override def stopStream(implicit context: SparkSession): Unit =
    context.streams.active.find(_.name == queryName).foreach(_.stop)
}
