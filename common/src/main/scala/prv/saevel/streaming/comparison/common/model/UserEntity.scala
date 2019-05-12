package prv.saevel.streaming.comparison.common.model

case class UserEntity(id: Long, address: String, accounts: Seq[AccountEntity])
