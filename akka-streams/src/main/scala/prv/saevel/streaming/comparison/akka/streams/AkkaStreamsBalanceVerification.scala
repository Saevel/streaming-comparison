package prv.saevel.streaming.comparison.akka.streams

import java.time.Duration

import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Keep
import org.apache.kafka.common.serialization._
import prv.saevel.streaming.comparison.common.model._
import prv.saevel.streaming.comparison.common.utils.{JsonFormats, StreamProcessor}

object AkkaStreamsBalanceVerification extends StreamProcessor[AkkaStreamsConfiguration, AkkaStreamsContext]
  with StreamJoins with KafkaIO with JsonFormats {

  import spray.json._

  private var optionalControl: Option[Consumer.Control] = None

  override def startStream(config: AkkaStreamsConfiguration)(implicit context: AkkaStreamsContext): Unit = {

    implicit val actorSystem = context.actorSystem
    implicit val materializer = context.materializer

    val consumerSts = consumerSettings[Array[Byte], String, ByteArrayDeserializer, StringDeserializer](
      config.kafka.bootstrapServers,
      "AkkaStreamsBalanceVerification",
      config.other.getConfig("akka.kafka.consumer"),
      new ByteArrayDeserializer,
      new StringDeserializer
    )

    val accounts = source(consumerSts, config.kafka.accountsTopic).map{ case (_, value) => JsonParser(value).convertTo[Account]}
    val transactions = source(consumerSts, config.kafka.transactionsTopic).map{ case (_, value) => JsonParser(value).convertTo[Transaction]}

    val transactionsByAccountId = transactions
      .groupBy(Int.MaxValue, {transaction => transaction.accountId})
      .fold((0L, Seq.empty[Transaction]))((accs, transaction) => (transaction.accountId, accs._2 :+ transaction))
      .mergeSubstreams

    optionalControl = Option(accounts
      .joinWith(transactionsByAccountId, config.joinDuration.asFiniteDuration){case (account, (acctId, _)) => account.id == acctId}
      .map { case (account, (_, transactions)) => (account.id, balanceReport(account, transactions))}
      .map { case (accountId, report) => (accountId.toString, report.toJson.toString)}
      .toMat(sink[String, String](
        producerSettings[String, String, StringSerializer, StringSerializer](
          config.kafka.bootstrapServers,
          config.other.getConfig("akka.kafka.producer"),
          new StringSerializer,
          new StringSerializer
        ), config.kafka.balanceReportsTopic
      ))(Keep.left)
      .run)
  }

  override def stopStream(implicit context: AkkaStreamsContext): Unit = optionalControl.foreach(_.shutdown)

  implicit class DurationConversion(private val source: Duration){

    import scala.concurrent.duration._

    def asFiniteDuration: FiniteDuration = source.toMillis millis
  }

  private[akka] def balanceReport(account: Account, transactions:Seq[Transaction]):AccountBalanceReport = {
    val transactionsResult = transactionsBalance(transactions)
    AccountBalanceReport(account.userId, account.id, account.balance, transactionsResult, Math.abs(transactionsResult - account.balance) <= 0.01)
  }

  private[akka] def transactionsBalance(transactions: Seq[Transaction]): Double = transactions.map {
    case Transaction(_, _, value, TransactionType.Insertion) => value
    case Transaction(_, _, value, TransactionType.Withdrawal) => (-1.0 * value)
  }.fold(0.0)(_+_)
}
