package prv.saevel.streaming.comparison.akka.streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import prv.saevel.streaming.comparison.common.utils.ContextProvider

trait AkkaStreamsContextProvider extends ContextProvider[AkkaStreamsConfiguration, AkkaStreamsContext]{
  override def withContext[T](config: AkkaStreamsConfiguration)(f: AkkaStreamsContext => T): T = {
    implicit val actorSystem = ActorSystem(config.applicationName, config.other)
    f(AkkaStreamsContext(actorSystem, ActorMaterializer()))
  }
}
