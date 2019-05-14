package prv.saevel.streaming.comparison.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import prv.saevel.streaming.comparison.common.utils.ContextProvider

trait FlinkContextProvider extends ContextProvider[FlinkConfiguration, StreamExecutionEnvironment]{
  override def withContext[T](config: FlinkConfiguration)(f: StreamExecutionEnvironment => T): T =
    f(StreamExecutionEnvironment.createLocalEnvironment())
}
