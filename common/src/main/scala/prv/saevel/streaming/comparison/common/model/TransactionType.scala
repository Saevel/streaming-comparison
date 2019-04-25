package prv.saevel.streaming.comparison.common.model

sealed trait TransactionType

case object Withdrawal

case object Transfer

case object Insertion
