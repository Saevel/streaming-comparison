package prv.saevel.streaming.comparison.common.model

case class TransactionType(kind: String)

object TransactionType {

  final val Withdrawal = "Withdrawal"

  final val Insertion = "Insertion"
}
