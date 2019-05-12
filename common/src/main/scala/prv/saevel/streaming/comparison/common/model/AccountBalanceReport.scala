package prv.saevel.streaming.comparison.common.model

case class AccountBalanceReport(userId: Long,
                                accountId: Long,
                                accountBalance: Double,
                                transactionsBalance: Double,
                                passed: Boolean)
