package prv.saevel.streaming.comparison.kafka.streams

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder
import prv.saevel.streaming.comparison.common.utils.{JsonFormats, StreamProcessor}
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import prv.saevel.streaming.comparison.common.model._

object KafkaStreamsBalanceVerification extends StreamProcessor[KafkaStreamsConfiguration, StreamsBuilder] with JsonFormats {

  import spray.json._
  import com.ovoenergy.kafka.serialization.spray._
  import org.apache.kafka.streams.scala.Serdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import prv.saevel.streaming.comparison.kafka.streams.Implicits._

  private implicit val accountSerializer = spraySerializer[Account]

  private implicit val accountDeserializer = sprayDeserializer[Account]

  private var handle: Option[KafkaStreams] = None

  override def startStream(config: KafkaStreamsConfiguration)(implicit context: StreamsBuilder): Unit = {

    handle = Option(
      accountsById(config, context)
        .join(transactionsBalancesByAccountId(config, context))(toAccountBalanceReport(_, _))
        .mapValues(_.toJson.toString)
        .to(config.kafka.balanceReportsTopic)
    ).map(_ => new KafkaStreams(context.build, config.toProperties))
  }

  override def stopStream(implicit context: StreamsBuilder): Unit = handle.foreach(_.close)

  private def accountsById(config: KafkaStreamsConfiguration, context: StreamsBuilder): KStream[Long, Account] =
    context
      .stream[Long, String](config.kafka.accountsTopic)
      .mapValues(value => JsonParser(value).convertTo[Account])

  private def transactionsBalancesByAccountId(config: KafkaStreamsConfiguration,
                                              context: StreamsBuilder): KTable[Long, Double] =
    context
      .stream[Long, String](config.kafka.transactionsTopic)
      .mapValues(value => JsonParser(value).convertTo[Transaction])
      .mapValues(_ match {
        case Transaction(_, _, value, TransactionType.Insertion) => value
        case Transaction(_, _, value, TransactionType.Withdrawal) => (-1) * value
      })
      .groupByKey
      .aggregate(0.0)((_, value, aggregate) => aggregate + value)

  private[streams] def toAccountBalanceReport(account: Account, transactionsBalance: Double): AccountBalanceReport =
    AccountBalanceReport(account.userId, account.id, account.balance, transactionsBalance, Math.abs(account.balance - transactionsBalance) <= 0.01)
}
