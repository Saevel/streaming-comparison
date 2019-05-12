package prv.saevel.streaming.comparison.common.config

import cats.data.ValidatedNel
import cats._
import cats.data._
import cats.implicits._
import com.typesafe.config.Config
import prv.saevel.streaming.comparison.common.utils.ConfigurationHelper

case class KafkaConfiguration(bootstrapServers: String,
                              originalUsersTopic: String,
                              usersOutputTopic: String,
                              accountsTopic: String,
                              transactionsTopic: String,
                              balanceReportsTopic: String,
                              statisticsTopic: String)

object KafkaConfiguration extends ConfigurationHelper {

  def apply(config: => Config): ErrorOr[KafkaConfiguration] =
    (config.validatedString("bootstrap.servers"),
      config.validatedString("topics.users.original"),
      config.validatedString("topics.users.output"),
      config.validatedString("topics.accounts"),
      config.validatedString("topics.transactions"),
      config.validatedString("topics.balance.reports"),
      config.validatedString("topics.statistics")).mapN(KafkaConfiguration.apply)
}
