package prv.saevel.streaming.comparison.common.model

sealed trait TransactionType

case object Withdrawal extends TransactionType

case object Insertion extends TransactionType
