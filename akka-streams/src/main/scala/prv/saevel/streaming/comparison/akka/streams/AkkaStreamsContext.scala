package prv.saevel.streaming.comparison.akka.streams

import akka.actor.ActorSystem
import akka.stream.Materializer

case class AkkaStreamsContext(actorSystem: ActorSystem, materializer: Materializer)
