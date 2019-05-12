package prv.saevel.streaming.comparison.common.utils

import prv.saevel.streaming.comparison.common.model._
import spray.json.{DefaultJsonProtocol, JsString, JsValue, RootJsonFormat}

trait JsonFormats extends DefaultJsonProtocol {

  implicit val originalUserFormat = jsonFormat6(OriginalUser.apply)

  implicit val userFormat = jsonFormat3(User.apply)

  implicit val accountFormat = jsonFormat3(Account.apply)

  implicit val transactionTypeFormat = new RootJsonFormat[TransactionType] {
    override def read(json: JsValue): TransactionType = json match {
      case JsString("Insertion") => Insertion
      case JsString("Withdrawal") => Withdrawal
      case other => throw new IllegalArgumentException(s"Cannot deserialize $other to a TransactionType")
    }

    override def write(obj: TransactionType): JsValue = obj match {
      case Insertion => JsString("Insertion")
      case Withdrawal => JsString("Withdrawal")
    }
  }

  implicit val transactionFormat = jsonFormat4(Transaction.apply)

  implicit val statisticFormat = jsonFormat3(Statistic.apply)

  implicit val balanceReportFormat = jsonFormat5(AccountBalanceReport.apply)
}
