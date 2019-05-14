package prv.saevel.streaming.comparison.common.tests.utils

import org.apache.kafka.common.serialization.Deserializer

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait StreamReader[Format[_], T] {

  def readStream[KeyType, DataType, KD <: Deserializer[_], VD <: Deserializer[_]](keyDeserializer: KD, valueDeserializer: VD)
                                                                                 (bootstrapServers: String,
                                                                                  topic: String,
                                                                                  groupId: String = "default",
                                                                                  timeout: Duration = 5 seconds,
                                                                                  maxIterations: Long = 100)
                                                                                 (implicit deserializer: Format[DataType],
                                                                                  ex: ExecutionContext): Future[Seq[(KeyType, DataType)]]
}
