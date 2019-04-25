package prv.saevel.streaming.comparison.common.model

case class Transaction(id: Long, accountId: Long, value: Double, transactionType: TransactionType)
