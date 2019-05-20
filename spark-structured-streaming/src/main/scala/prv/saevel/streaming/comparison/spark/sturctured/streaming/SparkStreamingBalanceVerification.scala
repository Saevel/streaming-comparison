package prv.saevel.streaming.comparison.spark.sturctured.streaming

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import prv.saevel.streaming.comparison.common.utils.StreamProcessor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import prv.saevel.streaming.comparison.common.model._

object SparkStreamingBalanceVerification extends StreamProcessor[SparkConfiguration, SparkSession] with SparkKafkaIO {

  private case class TransactionsBalance(accountId: Long, transactionsBalance: Double)

  private case class TransactionWithTimestamp(transactionId: Long, accountId: Long, value: Double, transactionType: String, timestamp: Timestamp)

  private case class AccountWithTimestamp(id: Long, userId: Long, balance: Double, timestamp: Timestamp)

  private val queryName = "BalanceVerification"

  private val originalUserSchema = StructType(Seq(
    StructField("id", LongType),
    StructField("name", StringType),
    StructField("surname", StringType),
    StructField("address", StringType),
    StructField("age", IntegerType),
    StructField("country", StringType)
  ))

  private val accountSchema = StructType(Seq(
    StructField("id", LongType),
    StructField("userId", LongType),
    StructField("balance", DoubleType)
  ))

  private val transactionSchema = StructType(Seq(
    StructField("transactionId", LongType),
    StructField("accountId", LongType),
    StructField("value", DoubleType),
    StructField("transactionType", ObjectType(classOf[TransactionType]))
  ))

  override def startStream(config: SparkConfiguration)(implicit context: SparkSession): Unit = {

    import context.implicits._

    val accounts: Dataset[AccountWithTimestamp] = fromKafka(config.kafka.accountsTopic, config)
      .select(from_json($"value".cast(StringType), accountSchema).as[Account], $"timestamp".as[Timestamp])
      .map { case (account, timestamp) => AccountWithTimestamp(account.id, account.userId, account.balance, timestamp) }
      .withWatermark("timestamp", s"${config.watermark.toMillis} milliseconds")
      .cache

    val transactions: Dataset[TransactionWithTimestamp] = fromKafka(config.kafka.transactionsTopic, config)
      .select(from_json($"value".cast(StringType), transactionSchema).as[Transaction], $"timestamp".as[Timestamp])
      .map { case (transaction, timestamp) => TransactionWithTimestamp(transaction.transactionId, transaction.accountId, transaction.value, transaction.transactionType, timestamp) }
      .withWatermark("timestamp", s"${config.watermark.toMillis} milliseconds")
      .cache

    accounts
      .joinWith(transactions, $"id" === transactions("accountId"))
      .cache
      .groupByKey { case (account, _) => account.id }
      .mapGroups { case (_, iterator) => (iterator.map(_._1).next, iterator.map(_._2)) }
      .map { case (account, transactions) => toReport(account, transactions) }
      .select($"accountId".cast(StringType).as("key"), to_json(struct("*")).as("value"))
      .toKafka(config.kafka.balanceReportsTopic, queryName, config)
  }


  override def stopStream(implicit context: SparkSession): Unit =
    context.streams.active.filter(_.name == queryName).foreach(_.stop)

  private def toReport(account: AccountWithTimestamp, transactions: Iterator[TransactionWithTimestamp]) = {
    val transactionsBalance: Double = transactions.map {
      case TransactionWithTimestamp(_, _, value, TransactionType.Insertion, _) => value
      case TransactionWithTimestamp(_, _, value, TransactionType.Withdrawal, _) => (-1) * value
    }.fold(0.0)(_+_)

    AccountBalanceReport(
      account.userId,
      account.id,
      account.balance,
      transactionsBalance,
      Math.abs(account.balance - transactionsBalance) <= 0.01
    )
  }
}
