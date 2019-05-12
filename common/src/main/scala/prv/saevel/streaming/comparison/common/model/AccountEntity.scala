package prv.saevel.streaming.comparison.common.model

case class AccountEntity(id: Long, balance: Double, transactions: Seq[Transaction])
