package prv.saevel.streaming.comparison.kafka.streams

import java.time.Duration

import cats.data.Validated.{Invalid, Valid}
import cats.data.ValidatedNel
import com.typesafe.config.ConfigFactory
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import prv.saevel.streaming.comparison.common.model._
import prv.saevel.streaming.comparison.common.utils.{ExternalConfiguration, JsonFormats}
import spray.json.{DefaultJsonProtocol, JsonParser}


object KafkaStreamsApplication extends JsonFormats with ExternalConfiguration with KafkaStreamsContextProvider {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._
  import org.apache.kafka.streams.scala._
  import spray.json._

  private val builder = new StreamsBuilder

  private val configuration: ValidatedNel[Throwable, KafkaStreamsConfiguration] = KafkaStreamsConfiguration(
    rawConfig.getOrElse(ConfigFactory.empty)
  )

  configuration match {
    case Valid(config) => withContext(config){ implicit streamsBuilder =>
      KafkaStreamsUserTransformation.startStream(config)
      // TODO: Run joins
      // TODO: Run aggregations
    }

    case Invalid(errorList) =>
      errorList.toList.foreach(_.printStackTrace)
  }

  /**
  def joinAndVerify(builder: StreamsBuilder) = {
    val usersStream: KStream[Long, User] = builder
      .stream[String, String]("TODO")
      .mapValues(s => JsonParser(s).convertTo[User])
      .selectKey{ case (_, user) => user.id}

    val accountsByUserId: KStream[Long, Account] = builder
      .stream[String, String]("TODO")
      .mapValues(s => JsonParser(s).convertTo[Account])
      .selectKey{ case (_, account) => account.userId}

    val transactionsVerifications: KStream[Long, Double] = builder
      .stream[String, String]("TODO")
      .mapValues(s => JsonParser(s).convertTo[Transaction])
      .selectKey{ case (_, transaction) => transaction.accountId}
      .groupByKey
      .aggregate(0.0)((_, transaction, accumulator) => transaction.transactionType match {
        case Insertion => accumulator + transaction.value
        case Withdrawal => accumulator - transaction.value
      })

    val usersAndAccounts = usersStream.join(accountsByUserId)(
      (user, account) => (user, account),
      JoinWindows.of(duration)
    ).selectKey{ case (userId, (user, account)) => account.id}
    */

    /**
    val accountsAndTransactionByUserId: KStream[Long, (Account, Transaction)] = accountsById.join(transactionsByAccountId)(
      (account, transaction) => (account, transaction),
      // TODO: JOIN WINDOW!
      JoinWindows.of(???)
    ).selectKey{ case (_, (account, _)) => account.userId}

    val joined: KStream[Long, (User, Account, Transaction)] = usersStream.join(accountsAndTransactionByUserId)(
      (user, accountsAndTransactions) => (user, accountsAndTransactions._1, accountsAndTransactions._2),
      // TODO: JOIN WINDOW!
      JoinWindows.of(???)
    ).group
      */
}
