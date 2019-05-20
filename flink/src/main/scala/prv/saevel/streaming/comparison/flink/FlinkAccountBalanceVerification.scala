package prv.saevel.streaming.comparison.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import prv.saevel.streaming.comparison.common.model._
import prv.saevel.streaming.comparison.common.utils.{JsonFormats, StreamProcessor}
import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.util.{Failure, Try}

object FlinkAccountBalanceVerification extends StreamProcessor[FlinkConfiguration, StreamExecutionEnvironment]
  with FlinkKafkaIO with JsonFormats {

  import org.apache.flink.streaming.api.scala._
  import spray.json._

  private var accountsConsumerAttempt: Try[FlinkKafkaConsumer[String]] = Failure(new NotImplementedException())

  private var transactionConsumerAttempt: Try[FlinkKafkaConsumer[String]] = Failure(new NotImplementedException())

  private var producerAttempt: Try[FlinkKafkaProducer[String]] = Failure(new NotImplementedException())

  override def startStream(config: FlinkConfiguration)(implicit context: StreamExecutionEnvironment): Unit = {
    accountsConsumerAttempt = Try(kafkaConsumer(config.kafka, config.kafka.accountsTopic, "FlinkAccountBalanceVerification"))
    transactionConsumerAttempt = Try(kafkaConsumer(config.kafka, config.kafka.transactionsTopic, "FlinkAccountBalanceVerification"))
    producerAttempt = Try(kafkaProducer(config.kafka, config.kafka.balanceReportsTopic))

    accountsConsumerAttempt.foreach(accountsConsumer =>
      transactionConsumerAttempt.foreach(transactionsConsumer =>
        producerAttempt.foreach { producer =>
          val accounts =
            context
              .addSource(accountsConsumer)
              .map(s => JsonParser(s).convertTo[Account])

          val transactionBalances =
            context
              .addSource(transactionsConsumer)
              .map(s => JsonParser(s).convertTo[Transaction])
              .keyBy(_.accountId).fold((0L, 0.0))((aggregate, transaction) => (
              transaction.accountId,
              transaction match {
                case Transaction(_, _, value, TransactionType.Insertion) => aggregate._2 + value
                case Transaction(_, _, value, TransactionType.Withdrawal) => aggregate._2 - value
              })
            )

          accounts
            .join(transactionBalances)
            .where(_.id)
            .equalTo{case (id, _) => id}
            .window(new TumblingProcessingTimeWindows(0L, config.joinDuration.toMillis))
            .apply((account, idAndBalance) => toBalanceReport(account, idAndBalance._2))
            .map(_.toJson.toString)
            .addSink(producer)
        }
      )
    )
  }

  override def stopStream(implicit context: StreamExecutionEnvironment): Unit = {
    accountsConsumerAttempt.foreach(_.close)
    transactionConsumerAttempt.foreach(_.close)
    producerAttempt.foreach(_.close)
  }

  private[flink] def toBalanceReport(account: Account, transactionsBalance: Double): AccountBalanceReport = AccountBalanceReport(
    account.userId, account.id, account.balance, transactionsBalance, Math.abs(account.balance - transactionsBalance) <= 0.01
  )
}
