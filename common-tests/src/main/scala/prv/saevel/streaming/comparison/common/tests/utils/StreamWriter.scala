package prv.saevel.streaming.comparison.common.tests.utils

import org.apache.kafka.common.serialization.Serializer

import scala.concurrent.{ExecutionContext, Future}

trait StreamWriter[Format[_], T] {

  def writeStream[KeyType, DataType, KS <: Serializer[_], VS <: Serializer[_]](keySerializer: KS, valueSerializer: VS)
                                    (bootstrapServers: String)
                                    (topic: String, data: Seq[(KeyType, DataType)])
                                    (implicit format: Format[DataType], ex: ExecutionContext): Future[_]
}
